package QLI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public interface HDFSClient {

    public FileStatus[] listStatus(Path path) throws IOException;
    public FSDataInputStream open(Path path) throws IOException;

    public Path[] roots();
}
