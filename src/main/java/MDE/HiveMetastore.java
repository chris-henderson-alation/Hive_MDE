package MDE;

import kerberos.Kerberos;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.io.InputStream;

public class HiveMetastore {

    // https://www.ibm.com/support/knowledgecenter/en/SSPT3X_3.0.0/com.ibm.swg.im.infosphere.biginsights.admin.doc/doc/kerberos_hive.html

    public static HiveMetaStoreClient connect(InputStream ... configurations) throws MetaException {
        return new HiveMetaStoreClient(newConf(configurations));
    }

    public static HiveMetaStoreClient connect(String username, InputStream ... configurations) throws MetaException {
        HiveConf conf = newConf(configurations);
        conf.set(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME.toString(), username);
        return new HiveMetaStoreClient(conf);
    }

    public static HiveMetaStoreClient connect(String username, String password, InputStream ... configurations) throws MetaException, LoginException, IOException {
        HiveConf conf = newConf(configurations);
        switch (conf.get("hadoop.security.authentication")) {
            case "kerberos":
                UserGroupInformation.loginUserFromSubject(Kerberos.kinit(username, password));
                break;
            case "simple":
                conf.set(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME.toString(), username);
                conf.set(HiveConf.ConfVars.METASTOREPWD.toString(), password);
                break;
        }
        return new HiveMetaStoreClient(conf);
    }

    private static HiveConf newConf(InputStream ... configurations) {
        HiveConf conf = new HiveConf();
        for (InputStream configuration : configurations) {
            conf.addResource(configuration);
        }
        return conf;
    }
}
