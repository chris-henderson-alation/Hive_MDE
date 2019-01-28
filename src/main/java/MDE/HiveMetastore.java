package MDE;

//import QLI.configuration.AlationHiveConfiguration;
import kerberos.Kerberos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

public class HiveMetastore {

    // https://www.ibm.com/support/knowledgecenter/en/SSPT3X_3.0.0/com.ibm.swg.im.infosphere.biginsights.admin.doc/doc/kerberos_hive.html

    private static final Pattern CONFIGS = Pattern.compile(".*\\.xml");

    public static HiveMetaStoreClient connect(HiveConf configurations) throws MetaException {
        return new HiveMetaStoreClient(configurations);
    }

    public static HiveMetaStoreClient connect(HiveConf configurations, String username) throws MetaException {
        configurations.set(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME.toString(), username);
        return new HiveMetaStoreClient(configurations);
    }

    public static HiveMetaStoreClient connect(HiveConf configurations, String username, String password) throws MetaException, LoginException, IOException {
        switch (configurations.get("hadoop.security.authentication")) {
            case "kerberos":
                UserGroupInformation.loginUserFromSubject(Kerberos.kinit(username, password));
                break;
            case "simple":
                configurations.set(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME.toString(), username);
                configurations.set(HiveConf.ConfVars.METASTOREPWD.toString(), password);
                break;
        }
        return new HiveMetaStoreClient(configurations);
    }

    private static HiveConf newConf(InputStream ... configurations) {
        HiveConf conf = new HiveConf();
        for (InputStream configuration : configurations) {
            conf.addResource(configuration);
        }
        return conf;
    }
}
