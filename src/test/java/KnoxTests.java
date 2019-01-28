import Configuration.AlationHiveConfiguration;
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
    public void kobe() throws Exception {
        Logger.getRootLogger().setLevel(Level.INFO);
        String dir = "/Users/chris.henderson/hack/Hive_MDE/matrix/jakes";
        Client c = ClientFactory.newClient(new AlationHiveConfiguration(dir), "mduser", "hyp3rbAd", "ip-10-11-21-224.alationdata.com");
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

    String escapePercent(String str) {
        return str.replaceAll("%", "%25");
    }
}
