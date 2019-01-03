import QLI.HDFS;
import QLI.HDFSClient;
import QLI.QuerLogIngestion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

public class QLITest {

    public class TestHDFS implements HDFSClient {

        private FileSystem fs;

        public TestHDFS() throws Exception {
            Configuration conf = new Configuration();
            for (InputStream c : QLITest.getConfigs("/Users/chris.henderson/hack/Hive_MDE/data")) {
                conf.addResource(c);
            }
            this.fs = new LocalFileSystem();
            this.fs.initialize(new URI("file:///"), conf);
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
            return new Path[]{
                    new Path("/Users/chris.henderson/hack/Hive_MDE/hdfs/cdh/history/done"),
                    new Path("/Users/chris.henderson/hack/Hive_MDE/hdfs/hdp/mr-history/done")
            };
        }
    }

    @Test
    public void tryitout() throws Exception {
//        HDFSClient client = new HDFS(QLITest.getConfigs("/Users/chris.henderson/hack/Hive_MDE/data"));
//        QuerLogIngestion qli = new QuerLogIngestion(client, null, null);
//        QuerLogIngestion qli = new QuerLogIngestion(client, LocalDate.of(2018, 12, 1), LocalDate.of(2018, 12, 2));
//        qli.search();
//        System.out.println(qli.logs);
//        System.out.println();
    }

    @Test
    public void tryitoutLocal() throws Exception {
        HDFSClient client = new TestHDFS();
        // Nope does'nt work lol check it
        Calendar earliest = new GregorianCalendar(2018, Calendar.FEBRUARY ,15);
        Calendar latest = new GregorianCalendar(2018, Calendar.DECEMBER, 14);
        earliest.setTimeZone(TimeZone.getTimeZone("UTC"));
        latest.setTimeZone(TimeZone.getTimeZone("UTC"));
        QuerLogIngestion qli = new QuerLogIngestion(client, earliest, latest);
        qli.search();
        System.out.println(qli.logs.size());
    }

    public static InputStream[] getConfigs(String path) throws Exception {
        File[] files = new File(path).listFiles();
        if (files == null) {
            throw new IOException("hate you, hate your both");
        }
        ArrayList<InputStream> confs = new ArrayList<InputStream>();
        for (File file : files) {
            if (!file.getAbsolutePath().endsWith(".xml")) {
                continue;
            }
            confs.add(new FileInputStream(file));
        }
        return confs.toArray(new InputStream[]{});
    }

}
