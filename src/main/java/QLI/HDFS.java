package QLI;

import kerberos.Kerberos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.io.InputStream;

public class HDFS implements HDFSClient {

    private final FileSystem fs;

    public HDFS(InputStream ...configurations) throws IOException {
        Configuration conf = new Configuration();
        for (InputStream configuration : configurations) {
            conf.addResource(configuration);
        }
        this.fs = WebHdfsFileSystem.get(WebHdfsFileSystem.getDefaultUri(conf), conf);
    }

    public HDFS(String user, InputStream ...configurations) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        for (InputStream configuration : configurations) {
            conf.addResource(configuration);
        }
        this.fs = WebHdfsFileSystem.get(WebHdfsFileSystem.getDefaultUri(conf), conf, user);
    }

    public HDFS(String username, String password, InputStream ... configurations) throws LoginException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        for (InputStream configuration : configurations) {
            conf.addResource(configuration);
        }
        // You really do have to do this before constructing the Metastore client as the Metastore client DOES
        // attempt tp connect upon construction.
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(Kerberos.kinit(username, password).getSubject());
        this.fs = WebHdfsFileSystem.get(WebHdfsFileSystem.getDefaultUri(conf), conf, username);
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
