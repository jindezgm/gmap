/*
 * @Author: jinde.zgm
 * @Date: 2020-07-29 06:10:23
 * @Descripttion:
 */

package gmap

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
)

// transport not only send message to other node, but also receive messages from other nodes.
type transport struct {
	ctx    context.Context    // Transport context.
	cancel context.CancelFunc // Transport cancel function
	gmap   *gmap              // gmap pointer
	server *grpc.Server       // GRPC server
	peers  []peer             // Peers, other gmap node.
}

// newTransport create transport.
func newTransport(gm *gmap, endpoints []string) *transport {
	// Create transport object.
	t := &transport{
		gmap:   gm,
		server: grpc.NewServer(),
		peers:  make([]peer, len(endpoints)),
	}
	// Create transport context.
	t.ctx, t.cancel = context.WithCancel(context.Background())
	// Create peers for transport
	for i := range endpoints {
		if i != int(gm.id-1) {
			// Initialize peer and run it.
			t.peers[i].endpoint, t.peers[i].gmap = endpoints[i], gm
			t.peers[i].msgc, t.peers[i].done = make(chan raftpb.Message, 1000), make(chan struct{})
			go t.peers[i].run()
		} else {
			// Split IP and port
			items := strings.Split(endpoints[i], ":")
			if len(items) != 2 {
				logger.Panicf("invalid peer endpoint:%s", endpoints[i])
			}
			// Listen specific port
			listener, err := net.Listen("tcp4", "0.0.0.0:"+items[1])
			if nil != err {
				logger.Panicf("listen on the port(%s) failed:%v", items[1], err)
			}
			// Run the transport GRPC service
			RegisterTransportServiceServer(t.server, t)
			go t.server.Serve(listener)
		}
	}

	return t
}

// reportUnreachable report unreachable, report snapshot failure if snapshot message.
func reportUnreachable(r *raftNode, msg *raftpb.Message) {
	r.ReportUnreachable(msg.To)
	if msg.Type == raftpb.MsgSnap {
		r.ReportSnapshot(msg.To, raft.SnapshotFailure)
	}
}

// send raft message to peer(msg.To)
func (t *transport) send(msgs []raftpb.Message) {
	for _, msg := range msgs {
		// msg.To is peer ID, msg.To-1 is peer index of peers slice.
		// msg.To==0 mean drop the message.
		// t.peers[msg.To].msgc==nil mean self.
		if to := msg.To - 1; int64(to) >= 0 && t.peers[to].msgc != nil {
			select {
			// Send message to peer.
			case t.peers[to].msgc <- msg:
			// Transport is closed.
			case <-t.ctx.Done():
			// t.peers[to].msgc is full.
			default:
				reportUnreachable(t.gmap.raft, &msg)
			}
		}
	}
}

// SendMessage implement TransportServiceServer.SendMessage(), handle raft message send by other peers.
func (t *transport) SendMessage(server TransportService_SendMessageServer) error {
	for {
		// Receive message.
		bytes, err := server.Recv()
		if nil != err {
			logger.Errorf("receive message failed:%v", err)
			return err
		}
		// Unmarshal raft message.
		var msg raftpb.Message
		pbutil.MustUnmarshal(&msg, bytes.Data)
		// Send message to raft.
		t.gmap.raft.Step(t.ctx, msg)
	}
}

// SendSnapshot implement TransportServiceServer.SendSnapshot(), handle raft snapshot message send by other peers.
func (t *transport) SendSnapshot(ctx context.Context, bytes *Bytes) (*Empty, error) {
	var msg raftpb.Message
	pbutil.MustUnmarshal(&msg, bytes.Data)
	t.gmap.raft.Step(t.ctx, msg)
	return &Empty{}, nil
}

// stop close transport.
func (t *transport) stop() {
	// Stop receive message from other peers.
	t.server.Stop()
	// Stop raft step.
	t.cancel()
	// Stop peers.
	for i := range t.peers {
		if i != int(t.gmap.id-1) {
			// Send stop signal and waiting for exit.
			close(t.peers[i].msgc)
			<-t.peers[i].done
		}
	}
}

// peer equivalent to node.
type peer struct {
	id       uint64              // Node ID.
	gmap     *gmap               // gmap pointer.
	endpoint string              // Node endpoint.
	msgc     chan raftpb.Message // Message channel.
	done     chan struct{}       // Closed flag.
}

// run receive message and send to the node by GRPC.
func (p *peer) run() {
	defer close(p.done)

	var err error
	var serviceClient TransportServiceClient
	var sendClient TransportService_SendMessageClient

	// Receive mssage.
	for msg := range p.msgc {
		// Connect node.
		if nil == serviceClient {
			conn, err := grpc.Dial(p.endpoint, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(2*time.Second))
			if nil != err {
				logger.Errorf("connect node(%s) failed:%v", p.endpoint, err)
				reportUnreachable(p.gmap.raft, &msg)
				continue
			}
			serviceClient = NewTransportServiceClient(conn)
		}

		var try int
	retry:
		// Create send stream client.
		if nil == sendClient {
			if sendClient, err = serviceClient.SendMessage(context.Background()); nil != err {
				logger.Errorf("create send client(%s) failed:%v", p.endpoint, err)
				reportUnreachable(p.gmap.raft, &msg)
				continue
			}
		}
		// Snapshot上 should be sent by creating a coroutine, because the snapshot may be very large,
		// and the sending time will be vary long, even affect heartbeat sending.
		if msg.Type == raftpb.MsgSnap {
			go func() {
				// Compress the sent snapshot with gzip.
				ctx, snap, opt := context.Background(), &Bytes{Data: pbutil.MustMarshal(&msg)}, grpc.UseCompressor(gzip.Name)
				if _, err := serviceClient.SendSnapshot(ctx, snap, opt); nil != err {
					logger.Errorf("send(%s) snapshot failed:%v", p.endpoint, err)
					p.gmap.raft.ReportSnapshot(msg.To, raft.SnapshotFailure)
				}
				// Report send snapshot finished to raft and reduce inflight snapshots count.
				p.gmap.raft.ReportSnapshot(p.id, raft.SnapshotFinish)
				atomic.AddInt64(&p.gmap.inflightSnapshots, -1)
			}()
		} else if err := sendClient.Send(&Bytes{Data: pbutil.MustMarshal(&msg)}); nil != err {
			logger.Errorf("send(%s) message failed:%v", p.endpoint, err)
			// Maybe the network connection exception, reconnect and try again.
			if try++; try >= 3 {
				reportUnreachable(p.gmap.raft, &msg)
			} else {
				sendClient = nil
				goto retry
			}
		}
	}
}
