package QLI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

public interface HDFSClient {

    public FileStatus[] listStatus(Path path) throws IOException;
    public FSDataInputStream open(Path path) throws IOException;

    public Path[] roots();

    default Configuration buildConf(InputStream[] configurations) {
        Configuration conf = new Configuration();
        for (InputStream configuration : configurations) {
            conf.addResource(configuration);
        }
        return conf;
    }
}
