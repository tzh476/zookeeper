package org.apache.zookeeper.server;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.Record;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.persistence.FileHeader;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;

@InterfaceAudience.Public
public class LogFormatter {
    private static final Logger LOG = LoggerFactory.getLogger(LogFormatter.class);

    
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("USAGE: LogFormatter log_file");
            System.exit(2);
        }
        FileInputStream fis = new FileInputStream(args[0]);
        BinaryInputArchive logStream = BinaryInputArchive.getArchive(fis);
        FileHeader fhdr = new FileHeader();
        fhdr.deserialize(logStream, "fileheader");

        if (fhdr.getMagic() != FileTxnLog.TXNLOG_MAGIC) {
            System.err.println("Invalid magic number for " + args[0]);
            System.exit(2);
        }
        System.out.println("ZooKeeper Transactional Log File with dbid "
                + fhdr.getDbid() + " txnlog format version "
                + fhdr.getVersion());

        int count = 0;
        while (true) {
            long crcValue;
            byte[] bytes;
            try {
                crcValue = logStream.readLong("crcvalue");

                bytes = logStream.readBuffer("txnEntry");
            } catch (EOFException e) {
                System.out.println("EOF reached after " + count + " txns.");
                return;
            }
            if (bytes.length == 0) {
                                                System.out.println("EOF reached after " + count + " txns.");
                return;
            }
            Checksum crc = new Adler32();
            crc.update(bytes, 0, bytes.length);
            if (crcValue != crc.getValue()) {
                throw new IOException("CRC doesn't match " + crcValue +
                        " vs " + crc.getValue());
            }
            TxnHeader hdr = new TxnHeader();
            Record txn = SerializeUtils.deserializeTxn(bytes, hdr);
            System.out.println(DateFormat.getDateTimeInstance(DateFormat.SHORT,
                    DateFormat.LONG).format(new Date(hdr.getTime()))
                    + " session 0x"
                    + Long.toHexString(hdr.getClientId())
                    + " cxid 0x"
                    + Long.toHexString(hdr.getCxid())
                    + " zxid 0x"
                    + Long.toHexString(hdr.getZxid())
                    + " " + TraceFormatter.op2String(hdr.getType()) + " " + txn);
            if (logStream.readByte("EOR") != 'B') {
                LOG.error("Last transaction was partial.");
                throw new EOFException("Last transaction was partial.");
            }
            count++;
        }
    }
}
