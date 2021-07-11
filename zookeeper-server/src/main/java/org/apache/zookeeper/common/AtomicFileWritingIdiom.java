package org.apache.zookeeper.common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;


public class AtomicFileWritingIdiom {

    public static interface OutputStreamStatement {

        public void write(OutputStream os) throws IOException;

    }

    public static interface WriterStatement {

        public void write(Writer os) throws IOException;

    }

    public AtomicFileWritingIdiom(File targetFile, OutputStreamStatement osStmt)  throws IOException {
        this(targetFile, osStmt, null);
    }

    public AtomicFileWritingIdiom(File targetFile, WriterStatement wStmt)  throws IOException {
        this(targetFile, null, wStmt);
    }

    private AtomicFileWritingIdiom(File targetFile, OutputStreamStatement osStmt, WriterStatement wStmt)  throws IOException {
        AtomicFileOutputStream out = null;
        boolean error = true;
        try {
            out = new AtomicFileOutputStream(targetFile);
            if (wStmt == null) {
                                osStmt.write(out);
            } else {
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
                                wStmt.write(bw);
                bw.flush();
            }
            out.flush();
                        error = false;
        } finally {
                        if (out != null) {
                if (error) {
                                                            out.abort();
                } else {
                                                            IOUtils.closeStream(out);
                }
            }
        }
    }

}
