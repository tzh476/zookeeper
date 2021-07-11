package org.apache.zookeeper.server.util;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SerializeUtilsTest {

    @Test
    public void testSerializeRequestRequestIsNull() {
        byte[] data = SerializeUtils.serializeRequest(null);
        assertNull(data);
    }

    @Test
    public void testSerializeRequestRequestHeaderIsNull() {
        Request request = new Request(0, 0, 0, null, null, 0);
        byte[] data = SerializeUtils.serializeRequest(request);
        assertNull(data);
    }

    @Test
    public void testSerializeRequestWithoutTxn() throws IOException {
                TxnHeader header = mock(TxnHeader.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                OutputArchive oa = (OutputArchive) args[0];
                oa.writeString("header", "test");
                return null;
            }
        }).when(header).serialize(any(OutputArchive.class), anyString());
        Request request = new Request(1, 2, 3, header, null, 4);

                byte[] data = SerializeUtils.serializeRequest(request);

                assertNotNull(data);
        verify(header).serialize(any(OutputArchive.class), eq("hdr"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        boa.writeString("header", "test");
        baos.close();
        assertArrayEquals(baos.toByteArray(), data);
    }

    @Test
    public void testSerializeRequestWithTxn() throws IOException {
                TxnHeader header = mock(TxnHeader.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                OutputArchive oa = (OutputArchive) args[0];
                oa.writeString("header", "test");
                return null;
            }
        }).when(header).serialize(any(OutputArchive.class), anyString());
        Record txn = mock(Record.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                OutputArchive oa = (OutputArchive) args[0];
                oa.writeString("record", "test");
                return null;
            }
        }).when(txn).serialize(any(OutputArchive.class), anyString());
        Request request = new Request(1, 2, 3, header, txn, 4);

                byte[] data = SerializeUtils.serializeRequest(request);

                assertNotNull(data);
        InOrder inOrder = inOrder(header, txn);
        inOrder.verify(header).serialize(any(OutputArchive.class), eq("hdr"));
        inOrder.verify(txn).serialize(any(OutputArchive.class), eq("txn"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        boa.writeString("header", "test");
        boa.writeString("record", "test");
        baos.close();
        assertArrayEquals(baos.toByteArray(), data);
    }
}
