package org.apache.zookeeper.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TxnLogProposalIterator implements Iterator<Proposal> {
    private static final Logger LOG = LoggerFactory
            .getLogger(TxnLogProposalIterator.class);

    public static final TxnLogProposalIterator EMPTY_ITERATOR = new TxnLogProposalIterator();

    private boolean hasNext = false;

    private TxnIterator itr;

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    
    @Override
    public Proposal next() {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        Proposal p = new Proposal();
        try {
            TxnHeader hdr = itr.getHeader();
            Record txn = itr.getTxn();
            hdr.serialize(boa, "hdr");
            if (txn != null) {
                txn.serialize(boa, "txn");
            }
            baos.close();

            QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, itr.getHeader()
                    .getZxid(), baos.toByteArray(), null);
            p.packet = pp;
            p.request = null;

                        hasNext = itr.next();

        } catch (IOException e) {
            LOG.error("Unable to read txnlog from disk", e);
            hasNext = false;
        }

        return p;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    
    public void close() {
        if(itr != null){
            try {
                itr.close();
            } catch (IOException ioe) {
                LOG.warn("Error closing file iterator", ioe);
            }
        }
    }

    private TxnLogProposalIterator() {
    }

    public TxnLogProposalIterator(TxnIterator itr) {
        if (itr != null) {
            this.itr = itr;
            hasNext = (itr.getHeader() != null);
        }
    }

}
