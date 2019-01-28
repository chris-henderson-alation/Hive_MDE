package QLI.client;

import kerberos.Kerberos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class HDFS implements Client {

    private static final Logger LOGGER = Logger.getLogger(HDFS.class.getName());

    private final FileSystem fs;

    private static final Pattern CONFIGS = Pattern.compile(".*\\.xml");
    private static final Pattern KRB5 = Pattern.compile("krb5\\.conf");

//    public HDFS(String configDirectory) throws Exception {
//        this.fs = this.initializeFilesystem(QLI.configuration.AlationHiveConfiguration.build(QLI.configuration.AlationHiveConfiguration.gather(new File(configDirectory), CONFIGS).get(CONFIGS)));
//    }
//
//    public HDFS(String configDirectory, String username) throws Exception {
//        this.fs = this.initializeFilesystem(QLI.configuration.AlationHiveConfiguration.build(QLI.configuration.AlationHiveConfiguration.gather(new File(configDirectory), CONFIGS).get(CONFIGS)), username);
//    }
//
//    public HDFS(String configDirectory, String username, String password) throws Exception {
//        Map<Pattern, List<File>> m  = QLI.configuration.AlationHiveConfiguration.gather(new File(configDirectory), CONFIGS, KRB5);
//        if (m.get(KRB5).size() > 0) {
//            Kerberos.setKrb5Conf(m.get(KRB5).get(0).getAbsolutePath());
//        }
//        AlationHiveConfiguration conf = QLI.configuration.AlationHiveConfiguration.build(QLI.configuration.AlationHiveConfiguration.gather(new File(configDirectory), CONFIGS).get(CONFIGS));
//        UserGroupInformation.setConfiguration(conf);
//        UserGroupInformation.loginUserFromSubject(Kerberos.kinit(username, password));
//        this.fs = this.initializeFilesystem(conf);
//    }

    public HDFS(Configuration configuration) throws Exception {
        this.fs = this.initializeFilesystem(configuration);
    }

    public HDFS(Configuration configuration, String username) throws Exception {
        this.fs = this.initializeFilesystem(configuration, username);
    }

    public HDFS(Configuration configuration, String username, String password) throws Exception {
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromSubject(Kerberos.kinit(username, password));
        this.fs = this.initializeFilesystem(configuration);
    }

    private FileSystem initializeFilesystem(Configuration conf) throws Exception {
        switch (conf.get("dfs.http.policy")) {
            case "HTTPS_ONLY":
            case "HTTP_AND_HTTPS":
                return SWebHdfsFileSystem.get(conf);
            default:
                return WebHdfsFileSystem.get(conf);
        }
    }

    private FileSystem initializeFilesystem(Configuration conf, String username) throws Exception {
        switch (conf.get("dfs.http.policy")) {
            case "HTTPS_ONLY":
            case "HTTP_AND_HTTPS":
                return SWebHdfsFileSystem.get(SWebHdfsFileSystem.getDefaultUri(conf), conf, username);
            default:
                return WebHdfsFileSystem.get(SWebHdfsFileSystem.getDefaultUri(conf), conf, username);
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
