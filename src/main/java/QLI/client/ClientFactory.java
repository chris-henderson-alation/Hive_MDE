package QLI.client;

public class ClientFactory {

    public static Client newClient(String configDirectory, String username, String password, String hostname) throws Exception {
        if (hostname != null) {
            return new ManuallyKerberizedClient(new QLI.client.Knox(hostname, username, password, configDirectory), username, password);
        }
        if (password != null && username != null) {
            return new HDFS(configDirectory, username, password);
        }
        if (username != null) {
            return new HDFS(configDirectory, username);
        }
        return new HDFS(configDirectory);
    }

}
