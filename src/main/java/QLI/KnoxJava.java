package QLI;

import com.google.gson.JsonArray;
import groovy.json.JsonSlurper;
import kerberos.Kerberos;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.groovy.json.internal.LazyMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.http.util.EntityUtils;
import org.apache.knox.gateway.shell.BasicResponse;
import org.apache.knox.gateway.shell.KnoxSession;
import org.apache.knox.gateway.shell.hdfs.Hdfs;
import org.apache.knox.gateway.shell.Credentials;

import org.apache.knox.gateway.shell.job.Hive;

import groovy.json.JsonSlurper;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import javax.security.auth.Subject;
import java.io.*;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.util.ArrayList;


public class KnoxJava implements HDFSClient {

    private static final Logger LOGGER = Logger.getLogger(KnoxJava.class.getName());

    Configuration conf;

    private final String username;
    private final String password;
    private final Subject subject;
    private final KnoxSession session;

    public KnoxJava(String username, String password, InputStream ... configurations) throws Exception {
        this.conf = buildConf(configurations);
        this.username = username;
        this.password = password;
        this.subject = Kerberos.kinit(username, password);
        this.session = KnoxSession.loginInsecure("https://ip-10-11-21-224.alationdata.com:8443/gateway/default", username, password);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        KnoxJava self = this;
        return Subject.doAs(this.subject, new PrivilegedAction<FileStatus[]>() {
            @Override
            public FileStatus[] run() {
                try {
                    return self.runListStatus(path);
                } catch (Exception e) {
                    System.out.println(ExceptionUtils.getStackTrace(e));
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private FileStatus[] runListStatus(Path path) throws Exception, IOException, URISyntaxException {
        String p = escapePercent(path.toString());
        String text = Hdfs.ls(this.session).dir(p).now().getString();
        return deser(path, text);
    }

    public static FileStatus[] deser(Path parent, String text) throws Exception {
        ArrayList<FileStatus> f = new ArrayList<>();
        JsonNode node;
        try {
            node = new ObjectMapper().readTree(text).get("FileStatuses").get("FileStatus");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        for (JsonNode n : node) {
            FileStatus derp = new FileStatus(
                    n.get("length").getLongValue(),
                    "DIRECTORY".equals(n.get("type").getTextValue()),
                    n.get("replication").getIntValue(),
                    n.get("blockSize").getLongValue(),
                    n.get("modificationTime").getLongValue(),
                    n.get("accessTime").getLongValue(),
                    new FsPermission((short)n.get("permission").getIntValue()),
                    n.get("owner").asText(),
                    n.get("group").asText(),
                    null,
                    new Path(parent.toString() + Path.SEPARATOR + n.get("pathSuffix").asText())
            );
            LOGGER.info("made " + derp.getPath());
            f.add(derp);
        }
        return f.toArray(new FileStatus[]{});
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        LOGGER.info("Opening " + path.toString());
        String escapedPath = escapePercent(path.toString());
        KnoxJava self = this;
        return Subject.doAs(this.subject, new PrivilegedAction<FSDataInputStream>() {
            @Override
            public FSDataInputStream run() {
                try {
                    BasicResponse resp = Hdfs.get(self.session).from(escapedPath).now();
                    return new FSDataInputStream(new AreYouNotSatisfied(IOUtils.toByteArray(resp.getStream())));
                } catch (Exception e) {
                    LOGGER.info(path.toString());
                    LOGGER.info(ExceptionUtils.getStackTrace(e));
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /**
     * escapePercent escapes all instances of "%" within the string with
     * its URL encoding "%25".
     *
     * The remote HDFS server has the habit of attempting to URL decode strings that it receives.
     * That is, the string "abc%38def" will become "abc(def" on the other side.
     *
     * That is all well and fine, until you realize that Hive has the habit of using "%" in its
     * log filenames. For example:
     *
     * "...1%2528Sta-1541454726684-1-0-SUCCEEDED..."
     *
     * Which results in web HDFS searching for "...1(28Sta-1541454726684-1-0-SUCCEEDED..."
     * if you are naive enough to feed the string it gave you back to itself.
     *
     * @param str
     * @return
     */
    private String escapePercent(String str) {
        return str.replaceAll("%", "%25");
    }

    @Override
    public Path[] roots() {
        return new Path[]{new Path(this.conf.get("mapreduce.jobhistory.done-dir"))};
    }

    public static class AreYouNotSatisfied extends ByteArrayInputStream implements PositionedReadable, Seekable {


        public AreYouNotSatisfied(byte[] buf) {
            super(buf);
        }

        public AreYouNotSatisfied(byte[] buf, int offset, int length) {
            super(buf, offset, length);
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            if (position >= buf.length)
                throw new IllegalArgumentException();
            if (position + length > buf.length)
                throw new IllegalArgumentException();
            if (length > buffer.length)
                throw new IllegalArgumentException();

            System.arraycopy(buf, (int) position, buffer, offset, length);
            return length;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            read(position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            read(position, buffer, 0, buffer.length);
        }

        @Override
        public void seek(long pos) throws IOException {
            if (mark != 0) {
                throw new IllegalStateException();
            }
            reset();
            long skipped = skip(pos);
            if (skipped != pos) {
                throw new IOException();
            }
        }

        @Override
        public long getPos() throws IOException {
            return pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }
    }
}
