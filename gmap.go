/*
 * @Author: jinde.zgm
 * @Date: 2020-07-23 20:54:01
 * @Descripttion:
 */

package gmap

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/pkg/idutil"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/pkg/wait"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

const (
	// defaultSnapshotCatchUpEntries is the number of entries applied between two snapshots.
	defaultSnapshotCount = 100000
	// defaultSnapshotCatchUpEntries is the number of entries for a slow follower
	// to catch-up after compacting the raft storage entries.
	// We expect the follower has a millisecond level latency with the leader.
	// The max throughput is around 10K. Keep a 5K entries is enough for helping
	// follower to catch up.
	defaultSnapshotCatchUpEntries uint64 = 5000
)

// applyResult is gmap apply raft entry result.
type applyResult struct {
	rev uint64          // Apply revision.
	err error           // Apply error
	pre revisionedValue // Previous value.
}

// gmap implemment Interface.
type gmap struct {
	inflightSnapshots int64               // Number of snapshots in send
	lead              uint64              // Leader ID
	id                uint64              // This node ID.
	ctx               context.Context     // gmap context.
	cancel            context.CancelFunc  // gmap cancel function.
	raft              *raftNode           // Raft node.
	raftStorage       *raft.MemoryStorage // Raft storage.
	idGen             *idutil.Generator   // ID generator or proposal
	wait              wait.Wait           //
	done              chan struct{}       //
	sets              sets                //
}

// New create gmap.Interface.
// @ endpoints:	All gmap nodes endpoint(127.0.0.1:12345)
// @ id: This gmap node endpoint index in the endpoints slice, must be [0, len(endpoints))
// @ values: All types of values stored in gmap, for example, "", []byte{}...
func New(ctx context.Context, endpoints []string, id int, values ...interface{}) (Interface, error) {
	// Check arguments.
	if len(endpoints) == 0 {
		endpoints, id = []string{"127.0.0.1:12345"}, 0
	} else if id < 0 || id >= len(endpoints) {
		return nil, fmt.Errorf("invalid id=%d not in[0, %d)", id, len(endpoints))
	}
	// In gmap, id==0 means ivnalid id, so need to add 1 to id.
	id++
	// Set default context if not specific.
	if nil == ctx {
		ctx = context.Background()
		logger.Info("use background context because context is not specified")
	}

	// Create gmap object.
	gm := &gmap{
		id:          uint64(id),
		done:        make(chan struct{}),
		wait:        wait.New(),
		raftStorage: raft.NewMemoryStorage(),
		idGen:       idutil.NewGenerator(uint16(id), time.Now()),
		sets:        make(sets, len(values)),
	}
	// Initialize sets.
	for i := range values {
		typ := reflect.TypeOf(values[i])
		gm.sets[typ.String()] = newSet(typ, gm)
	}
	// Create context for gmap.
	gm.ctx, gm.cancel = context.WithCancel(ctx)
	// Create raft node for gmap.
	gm.raft = newRaft(gm, endpoints)
	gm.raft.transport = newTransport(gm, endpoints)
	// Create coroutine to apply raft entries.
	go gm.run()

	return gm, nil
}

// Leader implement Interface.Leader()
func (gm *gmap) Leader() int {
	return int(atomic.LoadUint64(&gm.lead)) - 1
}

// Map implement Interface.Map()
func (gm *gmap) Map(value interface{}, name string) (Map, error) {
	set, exist := gm.sets[reflect.TypeOf(value).String()]
	if !exist {
		return nil, fmt.Errorf("the value type is not exist:%v", reflect.TypeOf(value))
	}

	return set.get(name), nil
}

// Close implemnt Interface.Close()
func (gm *gmap) Close() {
	gm.cancel()
	<-gm.done
}

// gmapProgress is gmap apply raft progress.
type gmapProgress struct {
	confState raftpb.ConfState
	snapi     uint64 // Applied snapshot index.
	appliedt  uint64 // Applied term
	appliedi  uint64 // Applied index.
}

// run apply raft entries and snapshot.
func (gm *gmap) run() {
	// Destruct gmap before exit.
	defer func() {
		gm.raft.Stop()
		close(gm.done)
	}()
	// Start gmap raft node.
	go gm.raft.run()
	// Apply entries and snapshot get from raft.
	var gmp gmapProgress
	for {
		select {
		// New apply.
		case ap := <-gm.raft.applyc:
			gm.applyAll(&gmp, &ap)
		// gmap is closed.
		case <-gm.ctx.Done():
			return
		}
	}
}

// applyAll apply snapshot and entries.
func (gm *gmap) applyAll(gmp *gmapProgress, apply *apply) {
	// Apply snapshot
	gm.applySnapshot(gmp, apply)
	// Apply entries.
	gm.applyEntries(gmp, apply)
	// Wait for the raft routine to finish the disk writes before triggering a snapshot.
	// or applied index might be greater than the last index in raft storage.
	// since the raft routine might be slower than apply routine.
	<-apply.notifyc
	// Create snapshot if necessary
	gm.triggerSnapshot(gmp)
}

// applySnapshot restore snapshot data into storage and update the raft apply progress.
func (gm *gmap) applySnapshot(gmp *gmapProgress, apply *apply) {
	// Check snapshot empty or not.
	if raft.IsEmptySnap(apply.snapshot) {
		return
	}
	// Write apply snapshot log.
	logger.Infof("applying snapshot at index %d...", gmp.snapi)
	defer func() { logger.Infof("finished applying incoming snapshot at index %d", gmp.snapi) }()
	// If the index of snapshot is smaller than the currently applied index, maybe raft is fault.
	if apply.snapshot.Metadata.Index < gmp.appliedi {
		logger.Panicf("snapshot index [%d] should > applied index[%d] + 1", apply.snapshot.Metadata.Index, gmp.appliedi)
	}
	// Because gmap does not need to be persistent, don't need wait for raftNode to persist snapshot into the disk.
	// <-apply.notifyc
	// Load storage data from snapshot.
	if err := gm.sets.load(apply.snapshot.Data); nil != err {
		logger.Panicf("storage load error:%v", err)
	}
	// Update gmap raft apply progress.
	gmp.appliedt, gmp.appliedi, gmp.confState = apply.snapshot.Metadata.Term, apply.snapshot.Metadata.Index, apply.snapshot.Metadata.ConfState
	gmp.snapi = gmp.appliedi
}

// applyEntries unmarshal proposal request and update storage data, raft apply progress.
func (gm *gmap) applyEntries(gmp *gmapProgress, apply *apply) {
	// Has entry?
	if len(apply.entries) == 0 {
		return
	}
	// Is the node leave the cluster tool long, the latest snapshot is better than the entry.
	firsti := apply.entries[0].Index
	if firsti > gmp.appliedi+1 {
		logger.Panicf("first index of committed entry[%d] should <= appliedi[%d] + 1", firsti, gmp.appliedi)
	}
	// Extract useful entries.
	var ents []raftpb.Entry
	if gmp.appliedi+1-firsti < uint64(len(apply.entries)) {
		ents = apply.entries[gmp.appliedi+1-firsti:]
	}
	// Iterate all entries
	for _, e := range ents {
		switch e.Type {
		// Normal entry.
		case raftpb.EntryNormal:
			if len(e.Data) != 0 {
				// Unmarshal request.
				var req InternalRaftRequest
				pbutil.MustUnmarshal(&req, e.Data)

				var ar applyResult
				// Put new value
				if put := req.Put; put != nil {
					// Get set.
					set, exist := gm.sets[put.Set]
					if !exist {
						logger.Panicf("set(%s) is not exist", put.Set)
					}
					// Get key, value and revision.
					key, value, revision := put.Key, set.vtype.unwrap(put.Value), e.Index
					// Get map and put value into map.
					m := set.get(put.Map)
					m.put(key, value, revision)
					// Send put event to watcher
					event := MapEvent{Type: PUT, KV: &KeyValue{Key: key, Value: value}}
					m.watchers.Range(func(key, value interface{}) bool {
						key.(*watcher).eventc <- event
						return true
					})
					// Set apply result.
					ar.rev = revision
				}
				// Delete value
				if del := req.Delete; del != nil {
					// Get set.
					set, exist := gm.sets[del.Set]
					if !exist {
						logger.Panicf("set(%s) is not exist", del.Set)
					}
					// Get map and delete value from map.
					m := set.get(del.Map)
					if pre := m.delete(del.Key); nil != pre {
						// Send put event to watcher
						ar.pre = *pre
						event := MapEvent{Type: DELETE, PrevKV: &KeyValue{Key: del.Key, Value: ar.pre.Value}}
						m.watchers.Range(func(key, value interface{}) bool {
							key.(*watcher).eventc <- event
							return true
						})
					}
				}
				// Update value
				if update := req.Update; update != nil {
					// Get set.
					set, exist := gm.sets[update.Set]
					if !exist {
						logger.Panicf("set(%s) is not exist", update.Set)
					}
					// Get map.
					m := set.get(update.Map)
					// Update value.
					pre, ok := m.update(update.Key, update.Value, update.Revision, e.Index)
					if ok {
						// The revision will be set only if update succeed
						ar.rev = e.Index
					}
					if nil != pre {
						ar.pre = *pre
					}
				}
				// Trigger proposal waiter.
				gm.wait.Trigger(req.ID, &ar)
			}
		// The configuration of gmap is fixed and wil not be synchronized through raft.
		case raftpb.EntryConfChange:
		default:
			logger.Panicf("entry type should be either EntryNormal or EntryConfChange")
		}

		gmp.appliedi, gmp.appliedt = e.Index, e.Term
	}
}

// triggerSnapshot create snapshot when a certain number of entries are applied.
func (gm *gmap) triggerSnapshot(gmp *gmapProgress) {
	if gmp.appliedi-gmp.snapi <= defaultSnapshotCount {
		return
	}
	logger.Infof("steart to snapshot (applied: %d, lastsnap: %d)", gmp.appliedi, gmp.snapi)
	gm.snapshot(gmp.appliedi, gmp.confState)
	gmp.snapi = gmp.appliedi
}

// snapshot create snapshot.
func (gm *gmap) snapshot(snapi uint64, confState raftpb.ConfState) {
	// Current store will never fail to do a snapshot what should we do if the store might fail?
	data, err := gm.sets.save()
	if nil != err {
		logger.Panicf("store save should never fail:%v", err)
	}
	// Save snapshot into raft storage.
	if _, err := gm.raftStorage.CreateSnapshot(snapi, &confState, data); nil != err {
		if err == raft.ErrSnapOutOfDate {
			return
		}
		logger.Panicf("unexpected create snapshot error %v", err)
	}
	// When sending a snapshot, gmap will pause compaction. After receives a snapshot, the slow follower needs
	// to get all the entries right after the snapshot sent to catch up. If we do not pause compaction, the
	// log entries right after the snapshot sent might already be compacted. It happens when the snapshot
	// take long to send and save. Pausing compaction avoids triggering a snapshot sending cycle.
	if atomic.LoadInt64(&gm.inflightSnapshots) != 0 {
		logger.Infof("skip compaction since there is an inflight snapshot")
		return
	}
	// Keep some in memory log entries for slow followers
	compacti := uint64(1)
	if snapi > defaultSnapshotCatchUpEntries {
		compacti = snapi - defaultSnapshotCatchUpEntries
	}
	// Compact raft storage.
	if err := gm.raftStorage.Compact(compacti); nil != err {
		if err == raft.ErrCompacted {
			return
		}
		logger.Panicf("unexpected compaction error %v", err)
	}

	logger.Infof("compacted raft log at %d", compacti)
}

// propose send proposal to raft and waiting for gmap applying result.
func (gm *gmap) propose(id uint64, proposal []byte) (*applyResult, error) {
	// Register propose ID in to waiting list.
	ch := gm.wait.Register(id)
	// 5s for queue waiting + 2 * election timeout for possible leader election.
	ctx, cancel := context.WithTimeout(gm.ctx, 5*time.Second+time.Duration(2*electionMs)*time.Millisecond)
	defer cancel()
	// Send proposal to raft.
	if err := gm.raft.Propose(ctx, proposal); nil != err {
		gm.wait.Trigger(id, nil)
		return nil, err
	}
	// Waiting for result.
	var err error
	select {
	// Propose result is return
	case resp := <-ch:
		return resp.(*applyResult), nil
	// Propose timeout
	case <-ctx.Done():
		err = ErrTimeout
		gm.wait.Trigger(id, nil)
	// gmap is closed.
	case <-gm.done:
		err = ErrStopped
	}

	return nil, err
}
