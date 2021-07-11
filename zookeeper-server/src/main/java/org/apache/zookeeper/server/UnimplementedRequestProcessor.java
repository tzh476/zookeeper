package org.apache.zookeeper.server;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.proto.ReplyHeader;


public class UnimplementedRequestProcessor implements RequestProcessor {

    public void processRequest(Request request) throws RequestProcessorException {
        KeeperException ke = new KeeperException.UnimplementedException();
        request.setException(ke);
        ReplyHeader rh = new ReplyHeader(request.cxid, request.zxid, ke.code().intValue());
        try {
            request.cnxn.sendResponse(rh, null, "response");
        } catch (IOException e) {
            throw new RequestProcessorException("Can't send the response", e);
        }

        request.cnxn.sendCloseSession();
    }

    public void shutdown() {
    }
}
