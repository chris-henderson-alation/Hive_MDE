package QLI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;

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
