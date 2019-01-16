import QLI.*;
import kerberos.Kerberos;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import javax.security.auth.Subject;
import java.io.InputStream;
import java.io.StringWriter;
import java.security.PrivilegedAction;

public class KnoxTests {

    @Test
    public void please() throws Exception {
        Knox client = new Knox();
        System.out.println(client.lol());
//        KnoxHDFS client = new KnoxHDFS(command.configs("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes"));
//        System.out.println(client.doThing());
    }

    @Test
    public void gotty() throws Exception {
        KnoxJava k = new KnoxJava("mduser", "hyp3rbAd", command.configs("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes"));
        FileStatus[] files = k.listStatus(new Path("/mr-history/"));
        for (FileStatus f : files) {
            System.out.println(f.toString());
        }
    }

    @Test
    public void kobe() throws Exception {
        Logger.getRootLogger().setLevel(Level.INFO);
        HDFSClient c = new KnoxJava("mduser", "hyp3rbAd", command.configs("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes"));
        QueryLogIngestion qli = new QueryLogIngestion(c, null, null, -1, null, 1);

        qli.out = new StringWriter();
        qli.search();
        System.out.println(qli.logs.size());
        System.out.println(qli.remoteExceptions);
        System.out.println(qli.ioExceptions);
        for (QueryLog log : qli.logs) {
            System.out.println(log);
        }
    }

    @Test
    public void open() throws Exception {
        Path a = new Path("/mr-history/done/2018/12/14/000000/job_1544814832146_0005-1544815942803-hdfs-SELECT+*+FROM+default.mystuff%0AI...mystuff.id%28S-1544815956168-1-0-SUCCEEDED-default-1544815948923.jhist");
        Path p = new Path("/mr-history/done/2018/12/14/000000/job_1544814832146_0005_conf.xml");
        HDFSClient c = new KnoxJava("mduser", "hyp3rbAd", command.configs("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes"));
        Logger.getRootLogger().setLevel(Level.TRACE);
        c.open(new Path(escapePercent(a.toString())));
;    }

    @Test
    public void badSig() throws Exception {
        Logger.getRootLogger().setLevel(Level.TRACE);
        Path a = new Path("/mr-history/done/2018/12/14/000000/job_1544814832146_0004_conf.xml");
        KnoxJava c = new KnoxJava("mduser", "hyp3rbAd", command.configs("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes"));
        InputStream s = c.open(a);
        System.out.println("asds");
    }

    String escapePercent(String str) {
        return str.replaceAll("%", "%25");
    }
}
