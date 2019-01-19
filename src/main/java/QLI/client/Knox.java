package QLI.client;

import kerberos.Kerberos;
import org.apache.commons.httpclient.URIException;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


public class Knox implements Client {

    private static final Logger LOGGER = Logger.getLogger(Knox.class.getName());

    public static final String GATEWAY_PORT = "gateway.port";
    public static final String GATEWAY_PATH = "gateway.path";


    private final Configuration conf;
    private final KnoxSession session;

//    public Knox(String username, String password, InputStream ... configurations) throws Exception {
//        this.conf = buildConf(configurations);
//        this.session = KnoxSession.loginInsecure("https://ip-10-11-21-224.alationdata.com:8443/gateway/default", username, password);
//    }


    private static final Pattern KEYSTORE = Pattern.compile(".*jks");
    private static final Pattern CONFIGS = Pattern.compile(".*xml");
    private static final Pattern KRB5 = Pattern.compile("krb5\\.conf");

    public Knox(String hostname, String username, String password, String configurationDirectory) throws URISyntaxException {
        Map<Pattern, List<File>> m = QLI.configuration.Configuration.gather(new File(configurationDirectory), CONFIGS, KRB5);
        if (m.get(KRB5).size() > 0) {
            Kerberos.setKrb5Conf(m.get(KRB5).get(0).getAbsolutePath());
        }
        this.conf = QLI.configuration.Configuration.build(m.get(CONFIGS));
        this.session = KnoxSession.loginInsecure(
                "https://" + hostname + ":" + this.conf.get(GATEWAY_PORT) + "/" + this.conf.get(GATEWAY_PATH) + "/default",
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

    private static Configuration buildConf(InputStream[] configurations) {
        Configuration conf = new Configuration();
        for (InputStream configuration : configurations) {
            conf.addResource(configuration);
        }
        return conf;
    }
}
