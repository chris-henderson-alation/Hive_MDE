package QLI.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

public interface Client {

    FileStatus[] listStatus(Path path) throws IOException;
    InputStream open(Path path) throws IOException;
    Path[] roots();

}
