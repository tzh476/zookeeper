package org.apache.zookeeper.server;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;

public class ByteBufferOutputStream extends OutputStream {
    ByteBuffer bb;
    public ByteBufferOutputStream(ByteBuffer bb) {
        this.bb = bb;
    }
    @Override
    public void write(int b) throws IOException {
        bb.put((byte)b);
    }
    @Override
    public void write(byte[] b) throws IOException {
        bb.put(b);
    }
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        bb.put(b, off, len);
    }
    static public void record2ByteBuffer(Record record, ByteBuffer bb)
    throws IOException {
        BinaryOutputArchive oa;
        oa = BinaryOutputArchive.getArchive(new ByteBufferOutputStream(bb));
        record.serialize(oa, "request");
    }
}
