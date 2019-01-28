package QLI.client;

import Configuration.AlationHiveConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.knox.gateway.shell.KnoxSession;
import org.apache.knox.gateway.shell.hdfs.Hdfs;


import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;


public class Knox implements Client {

    private static final Logger LOGGER = Logger.getLogger(Knox.class.getName());

    private final Configuration conf;
    private final KnoxSession session;

    public Knox(
            AlationHiveConfiguration conf,
            String hostname,
            String username,
            String password) throws URISyntaxException {
        this.conf = conf;
        this.session = KnoxSession.loginInsecure(
                "https://" +
                        hostname + ":" +
                        conf.get("gateway.port") + "/" +
                        conf.get("gateway.path") + "/" +
                        conf.webhdfsClusterName(),
                username, password);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        String p = escapePercent(path.toString());
        String text = Hdfs.ls(this.session).dir(p).now().getString();
        return deser(path, text);
    }

    @Override
    public InputStream open(Path path) throws IOException {
        String escapedPath = escapePercent(path.toString());
        return Hdfs.get(this.session).from(escapedPath).now().getStream();
    }

    @Override
    public Path[] roots() {
        return new Path[]{new Path(this.conf.get("mapreduce.jobhistory.done-dir"))};
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

    public static FileStatus[] deser(Path parent, String text) {
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
            f.add(derp);
        }
        return f.toArray(new FileStatus[]{});
    }
}
