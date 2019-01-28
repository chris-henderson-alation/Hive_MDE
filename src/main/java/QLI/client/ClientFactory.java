package QLI.client;

import Configuration.AlationHiveConfiguration;

public class ClientFactory {

    public static Client newClient(AlationHiveConfiguration conf, String username, String password, String knoxHostname) throws Exception {
        if (knoxHostname != null) {
            return new ManuallyKerberizedClient(
                    new QLI.client.Knox(conf, knoxHostname, username, password),
                    username, password);
        }
        if (password != null && username != null) {
            return new HDFS(conf, username, password);
        }
        if (username != null) {
            return new HDFS(conf, username);
        }
        return new HDFS(conf);
    }

}
