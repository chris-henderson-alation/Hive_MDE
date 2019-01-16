package QLI;

import kerberos.Kerberos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.logging.Logger;

public class HDFS implements HDFSClient {

    private static final Logger LOGGER = Logger.getLogger(HDFS.class.getName());

    private final FileSystem fs;

    public HDFS(InputStream ...configurations) throws IOException {
        Configuration conf = buildConf(configurations);
        this.fs = WebHdfsFileSystem.get(WebHdfsFileSystem.getDefaultUri(conf), conf);
    }

    public HDFS(String user, InputStream ...configurations) throws IOException, InterruptedException {
        Configuration conf = buildConf(configurations);
        this.fs = WebHdfsFileSystem.get(WebHdfsFileSystem.getDefaultUri(conf), conf, user);
    }

    public HDFS(String username, String password, InputStream ... configurations) throws LoginException, IOException, InterruptedException {
        Configuration conf = buildConf(configurations);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(Kerberos.kinit(username, password));
        this.fs = SWebHdfsFileSystem.get(conf);
        LOGGER.info("the URI is... " + this.fs.getUri().toString());
    }

    public static URI getUri() {
        try {
            return new URI("swebhdfs://ip-10-11-21-224.alationdata.com:8443/gateway/default/webhdfs/v1/");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return this.fs.listStatus(path);
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        return this.fs.open(path);
    }

    @Override
    public Path[] roots() {
        return new Path[]{new Path(this.fs.getConf().get("mapreduce.jobhistory.done-dir"))};
    }
}
