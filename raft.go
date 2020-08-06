/*
 * @Descripttion:
 * @Author: jinde.zgm
 * @Date: 2020-07-23 20:54:34
 * @LastEditors: Please set LastEditors
 */
package gmap

import (
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/raftpb"

	"go.etcd.io/etcd/pkg/contention"
	"go.etcd.io/etcd/raft"
)

var (
	maxSizePerMsg   uint64 = 1 * 1024 * 1024
	maxInflightMsgs int    = 4096 / 8
	tickMs                 = 500
	electionMs             = 2000
)

// raftNode is gmap raft node, processing ready data of raft
type raftNode struct {
	raft.Node
	gmap      *gmap
	applyc    chan apply
	stopped   chan struct{}
	done      chan struct{}
	ticker    *time.Ticker
	transport *transport
	td        *contention.TimeoutDetector
}

// newRaft create raft node of gmap.
func newRaft(gm *gmap, endpoints []string) *raftNode {
	// Create raft peers.
	peers := make([]raft.Peer, len(endpoints))
	for i, endpoint := range endpoints {
		peers[i].ID, peers[i].Context = uint64(i+1), []byte(endpoint)
	}

	// Create raft config.
	config := &raft.Config{
		ID:              gm.id,
		ElectionTick:    electionMs / tickMs,
		HeartbeatTick:   1,
		Storage:         gm.raftStorage,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         true,
		Logger:          logger,
	}

	return &raftNode{
		gmap:    gm,
		Node:    raft.StartNode(config, peers),
		applyc:  make(chan apply),
		stopped: make(chan struct{}),
		done:    make(chan struct{}),
		ticker:  time.NewTicker(time.Duration(tickMs) * time.Millisecond),
		td:      contention.NewTimeoutDetector(time.Duration(2*tickMs*config.HeartbeatTick) * time.Millisecond),
	}
}

// apply contains entries and snapshot to be applied. Once an apply is consumed, the entries will be persisted to
// to raft storage cocurrently.
type apply struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
	notifyc  chan struct{}
}

// run processing raft ready data.
func (r *raftNode) run() {
	defer func() {
		r.Stop()
		r.ticker.Stop()
		r.transport.stop()
		close(r.done)
	}()

	var islead bool
	for {
		select {
		// Tick raft.
		case <-r.ticker.C:
			r.Tick()
		// New ready data.
		case rd := <-r.Ready():
			// Update soft state, such as leader.
			if rd.SoftState != nil {
				atomic.StoreUint64(&r.gmap.lead, rd.SoftState.Lead)
				islead = rd.RaftState == raft.StateLeader
				r.td.Reset()
			}
			// Raft ready data needs to be persisted and applied in parallel,
			// applyc passes the snapshot and entries to the etcdserver to apply and persists them in this coroutine,
			// using notifyc to notify the completion of the persistence.
			// But gmap does not need persistent, just apply snapshots and entries to map and raft storage in parallel.
			// So notifyc is used to avoid applied index greater than the last index in raft storage before snapshot.
			notifyc := make(chan struct{}, 1)
			ap := apply{
				entries:  rd.CommittedEntries,
				snapshot: rd.Snapshot,
				notifyc:  notifyc,
			}
			// Send apply to gmap.
			select {
			case r.applyc <- ap:
			case <-r.stopped:
				return
			}
			// The leader can write ot its disk in parallel with replicating to the follower and them writing to their disks.
			if islead {
				r.transport.send(r.processMessage(rd.Messages))
			}
			// Process snapshot.
			if !raft.IsEmptySnap(rd.Snapshot) {
				// gmap no persistent, so don't notify.
				// notifyc <- struct{}{}

				// Write snapshot into raft storage.
				r.gmap.raftStorage.ApplySnapshot(rd.Snapshot)
				logger.Infof("raft applied incomming snapshot at index %d", rd.Snapshot.Metadata.Index)
			}
			// Write entries into raft storage.
			r.gmap.raftStorage.Append(rd.Entries)

			if !islead {
				ms := r.processMessage(rd.Messages)
				// Now unblocks 'applyAll' that waits on raft log disk writes before triggering snapshot.
				notifyc <- struct{}{}
				r.transport.send(ms)
			} else {
				// leader already processed 'MsgSnap' and signaled.
				notifyc <- struct{}{}
			}
			r.Advance()
		// raft node is closed.
		case <-r.stopped:
			return
		}
	}
}

// processMessage preprocess some message.
func (r *raftNode) processMessage(ms []raftpb.Message) []raftpb.Message {
	sendAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		// The response to the apply message only need to send the applied largest index.
		if ms[i].Type == raftpb.MsgAppResp {
			if sendAppResp {
				ms[i].To = 0 // ID=0 mean don't send this message.
			} else {
				sendAppResp = true
			}
		}
		// Add inflight snapshot count if snapshot message
		if ms[i].Type == raftpb.MsgSnap {
			atomic.AddInt64(&r.gmap.inflightSnapshots, 1)
		}
		// Check heartbeat message timeout.
		if ms[i].Type == raftpb.MsgHeartbeat {
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {
				logger.Warningf("failed to send out heartbeat on time (exceeded the %v timeout for %v, to %x)", tickMs, exceed, ms[i].To)
				logger.Warningf("server is likely overloaded")
			}
		}
	}

	return ms
}
