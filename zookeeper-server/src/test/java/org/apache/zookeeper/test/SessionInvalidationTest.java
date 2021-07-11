package org.apache.zookeeper.test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;


import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.RequestHeader;
import org.junit.Assert;
import org.junit.Test;

public class SessionInvalidationTest extends ClientBase {
    
    @Test
    public void testCreateAfterCloseShouldFail() throws Exception {
        for (int i = 0; i < 10; i++) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);

                        boa.writeInt(44, "len");
            ConnectRequest conReq = new ConnectRequest(0, 0, 30000, 0, new byte[16]);
            conReq.serialize(boa, "connect");

                        boa.writeInt(8, "len");
            RequestHeader h = new RequestHeader(1, ZooDefs.OpCode.closeSession);
            h.serialize(boa, "header");

                        boa.writeInt(52, "len");             RequestHeader header = new RequestHeader(2, OpCode.create);
            header.serialize(boa, "header");
            CreateRequest createReq = new CreateRequest("/foo" + i, new byte[0],
                    Ids.OPEN_ACL_UNSAFE, 1);
            createReq.serialize(boa, "request");
            baos.close();
            
            System.out.println("Length:" + baos.toByteArray().length);
            
            String hp[] = hostPort.split(":");
            Socket sock = new Socket(hp[0], Integer.parseInt(hp[1]));
            InputStream resultStream = null;
            try {
                OutputStream outstream = sock.getOutputStream();
                byte[] data = baos.toByteArray();
                outstream.write(data);
                outstream.flush();
                
                resultStream = sock.getInputStream();
                byte[] b = new byte[10000];
                int len;
                while ((len = resultStream.read(b)) >= 0) {
                                        System.out.println("gotlen:" + len);
                }
            } finally {
                if (resultStream != null) {
                    resultStream.close();
                }
                sock.close();
            }
        }
        
        ZooKeeper zk = createClient();
        Assert.assertEquals(1, zk.getChildren("/", false).size());

        zk.close();
    }
}
