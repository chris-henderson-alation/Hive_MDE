import Configuration.AlationHiveConfiguration;

import QLI.client.HDFS;
import QLI.client.Client;
import QLI.QueryLog;
import QLI.QueryLogIngestion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.*;

public class QLITest {

    public class TestHDFS implements Client {

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
//                    new Path("/Users/chris.henderson/hack/Hive_MDE/hdfs/cdh/history/done"),
//                    new Path("/Users/chris.henderson/hack/Hive_MDE/hdfs/hdp/mr-history/done"),
                    new Path("/Users/chris.henderson/hack/Hive_MDE/hdfs/mr-history/done")
            };
        }
    }

    @Test
    public void tryitout() throws Exception {
//        HDFSClient client = new HDFS(QLITest.getConfigs("/Users/chris.henderson/hack/Hive_MDE/data"));
//        QueryLogIngestion qli = new QueryLogIngestion(client, null, null);
//        QueryLogIngestion qli = new QueryLogIngestion(client, LocalDate.of(2018, 12, 1), LocalDate.of(2018, 12, 2));
//        qli.search();
//        System.out.println(qli.logs);
//        System.out.println();
    }

    @Test
    public void tryitoutLocal() throws Exception {
        Client client = new TestHDFS();
        // Nope does'nt work lol check it
//        Calendar earliest = new GregorianCalendar(2018, Calendar.FEBRUARY ,15);
//        Calendar latest = new GregorianCalendar(2018, Calendar.DECEMBER, 15);
//        earliest.setTimeZone(TimeZone.getTimeZone("UTC"));
//        latest.setTimeZone(TimeZone.getTimeZone("UTC"));
//        QueryLogIngestion qli = new QueryLogIngestion(client, earliest, latest);
        QueryLogIngestion qli = new QueryLogIngestion(client);
        Writer lol = new StringWriter();
        qli.out = lol;
        qli.search();
        System.out.println(qli.logs.size());
        System.out.println(qli.remoteExceptions.size());
        for (QueryLog q : qli.logs) {
            if (q.getSessionId() == null) {
                System.out.println("lol");
            }
        }
        System.out.println(lol.toString());
    }

    public static InputStream[] getConfigs(String path) throws Exception {
        return command.configs(path);
    }

    @Test
    public void fuck() throws Exception {
        AlationHiveConfiguration configs = new AlationHiveConfiguration("/Users/chris.henderson/hack/Hive_MDE/matrix/knox");
        Client c = new HDFS(configs, "hive");
        QueryLogIngestion qli = new QueryLogIngestion(c);
        Logger.getRootLogger().setLevel(Level.DEBUG);
        qli.search();
        System.out.println(qli.logs.size());
        System.out.println(qli.remoteExceptions.size());
    }

    @Test
    public void fuckme() throws Exception {
        AlationHiveConfiguration configs = new AlationHiveConfiguration("/Users/chris.henderson/hack/Hive_MDE/matrix/knoxKerberos");
        Client c = new HDFS(configs, "mduser", "hyp3rbAd");
        QueryLogIngestion qli = new QueryLogIngestion(c);
        Logger.getRootLogger().setLevel(Level.DEBUG);
        qli.search();
        System.out.println(qli.logs.size());
        System.out.println(qli.remoteExceptions);
        System.out.println(qli.ioExceptions);
    }

    @Test
    public void knox() throws Exception {
        AlationHiveConfiguration configs = new AlationHiveConfiguration("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes");
        Client c = new HDFS(configs, "mduser", "hyp3rbAd");
        QueryLogIngestion qli = new QueryLogIngestion(c);
        Logger.getRootLogger().setLevel(Level.INFO);
        qli.out = new StringWriter();
        qli.search();
        System.out.println(qli.logs.size());
        System.out.println(qli.remoteExceptions);
        System.out.println(qli.ioExceptions);
    }

    @Test
    public void azure() throws Exception {
        AlationHiveConfiguration configs = new AlationHiveConfiguration("/Users/chris.henderson/hack/Hive_MDE/matrix/azure");
        Client c = new HDFS(configs);
        QueryLogIngestion qli = new QueryLogIngestion(c);
        Logger.getRootLogger().setLevel(Level.INFO);
        qli.out = new StringWriter();
        qli.search();
        System.out.println(qli.logs.size());
        System.out.println(qli.remoteExceptions);
        System.out.println(qli.ioExceptions);
    }

}
