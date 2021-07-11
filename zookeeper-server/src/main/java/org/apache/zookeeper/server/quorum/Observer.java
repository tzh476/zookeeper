package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.jute.Record;
import org.apache.zookeeper.server.ObserverBean;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;


public class Observer extends Learner{

    Observer(QuorumPeer self,ObserverZooKeeperServer observerZooKeeperServer) {
        this.self = self;
        this.zk=observerZooKeeperServer;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Observer ").append(sock);
        sb.append(" pendingRevalidationCount:")
            .append(pendingRevalidations.size());
        return sb.toString();
    }

    
    void observeLeader() throws Exception {
        zk.registerJMX(new ObserverBean(this, zk), self.jmxLocalPeerBean);

        try {
            QuorumServer leaderServer = findLeader();
            LOG.info("Observing " + leaderServer.addr);
            try {
                connectToLeader(leaderServer.addr, leaderServer.hostname);
                long newLeaderZxid = registerWithLeader(Leader.OBSERVERINFO);
                if (self.isReconfigStateChange())
                   throw new Exception("learned about role change");
 
                syncWithLeader(newLeaderZxid);
                QuorumPacket qp = new QuorumPacket();
                while (this.isRunning()) {
                    readPacket(qp);
                    processPacket(qp);
                }
            } catch (Exception e) {
                LOG.warn("Exception when observing the leader", e);
                try {
                    sock.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

                                pendingRevalidations.clear();
            }
        } finally {
            zk.unregisterJMX(this);
        }
    }

    
    protected void processPacket(QuorumPacket qp) throws Exception{
        switch (qp.getType()) {
        case Leader.PING:
            ping(qp);
            break;
        case Leader.PROPOSAL:
            LOG.warn("Ignoring proposal");
            break;
        case Leader.COMMIT:
            LOG.warn("Ignoring commit");
            break;
        case Leader.UPTODATE:
            LOG.error("Received an UPTODATE message after Observer started");
            break;
        case Leader.REVALIDATE:
            revalidate(qp);
            break;
        case Leader.SYNC:
            ((ObserverZooKeeperServer)zk).sync();
            break;
        case Leader.INFORM:
            TxnHeader hdr = new TxnHeader();
            Record txn = SerializeUtils.deserializeTxn(qp.getData(), hdr);
            Request request = new Request (hdr.getClientId(),  hdr.getCxid(), hdr.getType(), hdr, txn, 0);
            ObserverZooKeeperServer obs = (ObserverZooKeeperServer)zk;
            obs.commitRequest(request);
            break;
        case Leader.INFORMANDACTIVATE:            
            hdr = new TxnHeader();
            
                       ByteBuffer buffer = ByteBuffer.wrap(qp.getData());    
           long suggestedLeaderId = buffer.getLong();
           
            byte[] remainingdata = new byte[buffer.remaining()];
            buffer.get(remainingdata);
            txn = SerializeUtils.deserializeTxn(remainingdata, hdr);
            QuorumVerifier qv = self.configFromString(new String(((SetDataTxn)txn).getData()));
            
            request = new Request (hdr.getClientId(),  hdr.getCxid(), hdr.getType(), hdr, txn, 0);
            obs = (ObserverZooKeeperServer)zk;
                        
            boolean majorChange = 
                self.processReconfig(qv, suggestedLeaderId, qp.getZxid(), true);
           
            obs.commitRequest(request);                                 

            if (majorChange) {
               throw new Exception("changes proposed in reconfig");
           }            
            break;
        default:
            LOG.warn("Unknown packet type: {}", LearnerHandler.packetToString(qp));
            break;
        }
    }

    
    public void shutdown() {
        LOG.info("shutdown called", new Exception("shutdown Observer"));
        super.shutdown();
    }
}

