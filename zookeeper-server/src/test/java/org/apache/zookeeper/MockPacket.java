package org.apache.zookeeper;

import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooKeeper.WatchRegistration;
import java.nio.ByteBuffer;

public class MockPacket extends ClientCnxn.Packet {

    public MockPacket(RequestHeader requestHeader, ReplyHeader replyHeader,
               Record request, Record response,
               WatchRegistration watchRegistration) {
        super(requestHeader, replyHeader, request, response, watchRegistration);
    }

    public MockPacket(RequestHeader requestHeader, ReplyHeader replyHeader,
               Record request, Record response,
               WatchRegistration watchRegistration, boolean readOnly) {
        super(requestHeader, replyHeader, request, response, watchRegistration, readOnly);
    }

    public ByteBuffer createAndReturnBB() {
        createBB();
        return this.bb;
    }

}
