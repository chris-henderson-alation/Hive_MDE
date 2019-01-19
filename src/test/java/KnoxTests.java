import MDE.KnoxMeCaptain;
import QLI.*;
import QLI.client.Client;
import QLI.client.ClientFactory;
import kerberos.Kerberos;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.StringWriter;
import java.security.PrivilegedExceptionAction;

public class KnoxTests {

    @Test
    public void please() throws Exception {
//        Knox client = new Knox();
//        System.out.println(client.lol());
//        KnoxHDFS client = new KnoxHDFS(command.configs("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes"));
//        System.out.println(client.doThing());
    }

//    @Test
//    public void gotty() throws Exception {
//        Knox k = new Knox("mduser", "hyp3rbAd", command.configs("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes"));
//        FileStatus[] files = k.listStatus(new Path("/mr-history/"));
//        for (FileStatus f : files) {
//            System.out.println(f.toString());
//        }
//    }

    @Test
    public void kobe() throws Exception {
        Logger.getRootLogger().setLevel(Level.INFO);
        Client c = ClientFactory.newClient("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes", "mduser", "hyp3rbAd", "ip-10-11-21-224.alationdata.com");
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

//    @Test
//    public void open() throws Exception {
//        Path a = new Path("/mr-history/done/2018/12/14/000000/job_1544814832146_0005-1544815942803-hdfs-SELECT+*+FROM+default.mystuff%0AI...mystuff.id%28S-1544815956168-1-0-SUCCEEDED-default-1544815948923.jhist");
//        Path p = new Path("/mr-history/done/2018/12/14/000000/job_1544814832146_0005_conf.xml");
//        HDFSClient c = new Knox("mduser", "hyp3rbAd", command.configs("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes"));
//        Logger.getRootLogger().setLevel(Level.TRACE);
//        c.open(new Path(escapePercent(a.toString())));
//;    }
//
//    @Test
//    public void badSig() throws Exception {
//        Logger.getRootLogger().setLevel(Level.TRACE);
//        Path a = new Path("/mr-history/done/2018/12/14/000000/job_1544814832146_0004_conf.xml");
//        Knox c = new Knox("mduser", "hyp3rbAd", command.configs("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes"));
//        InputStream s = c.open(a);
//        System.out.println("asds");
//    }

    String escapePercent(String str) {
        return str.replaceAll("%", "%25");
    }

    @Test
    public void JDBC() throws Exception {
        command.configs("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes");
        Subject subject = Kerberos.kinit("mduser", "hyp3rbAd");
        Subject.doAs(subject, (PrivilegedExceptionAction<Integer>) () -> { KnoxMeCaptain.doIt(); return 1;});
    }
}
