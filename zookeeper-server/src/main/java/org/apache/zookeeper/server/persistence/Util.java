package org.apache.zookeeper.server.persistence;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.txn.TxnHeader;


public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);
    private static final String SNAP_DIR="snapDir";
    private static final String LOG_DIR="logDir";
    private static final String DB_FORMAT_CONV="dbFormatConversion";
    
    public static String makeURIString(String dataDir, String dataLogDir, 
            String convPolicy){
        String uri="file:"+SNAP_DIR+"="+dataDir+";"+LOG_DIR+"="+dataLogDir;
        if(convPolicy!=null)
            uri+=";"+DB_FORMAT_CONV+"="+convPolicy;
        return uri.replace('\\', '/');
    }
    
    public static URI makeFileLoggerURL(File dataDir, File dataLogDir){
        return URI.create(makeURIString(dataDir.getPath(),dataLogDir.getPath(),null));
    }
    
    public static URI makeFileLoggerURL(File dataDir, File dataLogDir,String convPolicy){
        return URI.create(makeURIString(dataDir.getPath(),dataLogDir.getPath(),convPolicy));
    }

    
    public static String makeLogName(long zxid) {
        return FileTxnLog.LOG_FILE_PREFIX + "." + Long.toHexString(zxid);
    }

    
    public static String makeSnapshotName(long zxid) {
        return FileSnap.SNAPSHOT_FILE_PREFIX + "." + Long.toHexString(zxid);
    }
    
    
    public static File getSnapDir(Properties props){
        return new File(props.getProperty(SNAP_DIR));
    }

    
    public static File getLogDir(Properties props){
        return new File(props.getProperty(LOG_DIR));
    }
    
    
    public static String getFormatConversionPolicy(Properties props){
        return props.getProperty(DB_FORMAT_CONV);
    }
   
    
    public static long getZxidFromName(String name, String prefix) {
        long zxid = -1;
        String nameParts[] = name.split("\\.");
        if (nameParts.length == 2 && nameParts[0].equals(prefix)) {
            try {
                zxid = Long.parseLong(nameParts[1], 16);
            } catch (NumberFormatException e) {
            }
        }
        return zxid;
    }

    
    public static boolean isValidSnapshot(File f) throws IOException {
        if (f==null || Util.getZxidFromName(f.getName(), FileSnap.SNAPSHOT_FILE_PREFIX) == -1)
            return false;

                try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
                                    if (raf.length() < 10) {
                return false;
            }
            raf.seek(raf.length() - 5);
            byte bytes[] = new byte[5];
            int readlen = 0;
            int l;
            while (readlen < 5 &&
                    (l = raf.read(bytes, readlen, bytes.length - readlen)) >= 0) {
                readlen += l;
            }
            if (readlen != bytes.length) {
                LOG.info("Invalid snapshot " + f
                        + " too short, len = " + readlen);
                return false;
            }
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            int len = bb.getInt();
            byte b = bb.get();
            if (len != 1 || b != '/') {
                LOG.info("Invalid snapshot " + f + " len = " + len
                        + " byte = " + (b & 0xff));
                return false;
            }
        }

        return true;
    }

    
    public static byte[] readTxnBytes(InputArchive ia) throws IOException {
        try{
            byte[] bytes = ia.readBuffer("txtEntry");
                                    if (bytes.length == 0)
                return bytes;
            if (ia.readByte("EOF") != 'B') {
                LOG.error("Last transaction was partial.");
                return null;
            }
            return bytes;
        }catch(EOFException e){}
        return null;
    }
    

    
    public static byte[] marshallTxnEntry(TxnHeader hdr, Record txn)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputArchive boa = BinaryOutputArchive.getArchive(baos);

        hdr.serialize(boa, "hdr");
        if (txn != null) {
            txn.serialize(boa, "txn");
        }
        return baos.toByteArray();
    }

    
    public static void writeTxnBytes(OutputArchive oa, byte[] bytes)
            throws IOException {
        oa.writeBuffer(bytes, "txnEntry");
        oa.writeByte((byte) 0x42, "EOR");     }
    
    
    
    private static class DataDirFileComparator
        implements Comparator<File>, Serializable
    {
        private static final long serialVersionUID = -2648639884525140318L;

        private String prefix;
        private boolean ascending;
        public DataDirFileComparator(String prefix, boolean ascending) {
            this.prefix = prefix;
            this.ascending = ascending;
        }

        public int compare(File o1, File o2) {
            long z1 = Util.getZxidFromName(o1.getName(), prefix);
            long z2 = Util.getZxidFromName(o2.getName(), prefix);
            int result = z1 < z2 ? -1 : (z1 > z2 ? 1 : 0);
            return ascending ? result : -result;
        }
    }
    
    
    public static List<File> sortDataDir(File[] files, String prefix, boolean ascending)
    {
        if(files==null)
            return new ArrayList<File>(0);
        List<File> filelist = Arrays.asList(files);
        Collections.sort(filelist, new DataDirFileComparator(prefix, ascending));
        return filelist;
    }

    
    public static boolean isLogFileName(String fileName) {
        return fileName.startsWith(FileTxnLog.LOG_FILE_PREFIX + ".");
    }

    
    public static boolean isSnapshotFileName(String fileName) {
        return fileName.startsWith(FileSnap.SNAPSHOT_FILE_PREFIX + ".");
    }
    
}
