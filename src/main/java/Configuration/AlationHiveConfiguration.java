package Configuration;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;
import org.unix4j.Unix4j;
import org.unix4j.unix.Grep;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


import javax.xml.parsers.*;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class AlationHiveConfiguration extends HiveConf {

    private static final Logger LOGGER = Logger.getLogger(AlationHiveConfiguration.class.getName());

    private final HashMap<String, Document> topologies = new HashMap<>();
    private String clusterName;
    private String krb5;

    public AlationHiveConfiguration(String directory) {
        super();
        this.gather(directory);
    }

    private void gather(String directory) {
        this.gatherHiveConfs(directory);
        this.gatherTopologies(directory);
        this.gatherKerberosConfs(directory);
    }


    public String getKrb5() {
        return this.krb5;
    }

    public String webhdfsClusterName() {
        if (this.clusterName != null) {
            return this.clusterName;
        }
        String http = this.get("dfs.namenode.http-address");
        String https = this.get("dfs.namenode.https-address");
        for (String cluster : this.topologies.keySet()) {
            Document topology = this.topologies.get(cluster);
            if (clusterContainsWebhdfs(topology, http, https)) {
                this.clusterName = cluster;
                return this.clusterName;
            }
        }
        throw new RuntimeException("tantrums");
    }

    private void gatherHiveConfs(String directory) {
        List<String> hiveConfs = findPipeXargsGrep(directory, "*xml", "<configuration>");
        for (InputStream hiveConf : openAll(hiveConfs.toArray(new String[]{}))) {
            this.addResource(hiveConf);
        }
    }

    private void gatherTopologies(String directory) {
        DocumentBuilder xmlBuilder;
        try {
            xmlBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            LOGGER.fatal("failed to instantiate a new XML parser", e);
            System.exit(1);
            return;
        }
        List<String> topologies = findPipeXargsGrep(directory, "*xml", "<topology>");
        for (String fname : topologies) {
            try {
                String cluster = FilenameUtils.removeExtension(FilenameUtils.getBaseName(fname));
                this.topologies.put(cluster, xmlBuilder.parse(open(fname)));
            } catch (SAXException | IOException e) {
                LOGGER.fatal("failed to parse topology file " + fname, e);
                System.exit(1);
            }
        }
    }

    private void gatherKerberosConfs(String directory) {
        List<String> krb5s = find(directory,"krb5.conf");
        if (krb5s.size() == 0) {
            return;
        }
        if (krb5s.size() > 1) {
            LOGGER.warn("more than one krb5.conf was found");
            for (String krb5 : krb5s) {
                LOGGER.warn("\t" + krb5);
            }
            LOGGER.warn("selecting " + krb5s.get(0));
        }
        this.krb5 = krb5s.get(0);
    }

    private static List<String> find(String location, String pattern) {
        return Unix4j.find(location, pattern).toStringList();
    }

    private static List<String> findPipeXargsGrep(String location, String findPattern, String grepPattern) {
        return Unix4j.grep(Grep.Options.l, grepPattern, find(location, findPattern).toArray(new String[]{})).toStringList();
    }


    private static InputStream[] openAll(String[] listing) {
        ArrayList<InputStream> files = new ArrayList<>();
        for (String fname : listing) {
            files.add(open(fname));
        }
        return files.toArray(new InputStream[]{});
    }

    private static InputStream open(String fname) {
        try {
            return new FileInputStream(fname);
        } catch (IOException e) {
            LOGGER.fatal("found the following file, but failed to open it: " + fname, e);
            System.exit(1);
            return null;
        }
    }

    private static boolean clusterContainsWebhdfs(Document cluster, String webhdfs, String swebhdfs) {
        NodeList services = cluster.getElementsByTagName("service");
        for (int i = 0; i < services.getLength(); i++) {
            Node service = services.item(i);
            NodeList children = service.getChildNodes();
            for (int j = 0; j < children.getLength(); j++) {
                Node child = children.item(j);
                if ("url".equals(child.getNodeName())) {
                    try {
                        URL url = new URL(child.getTextContent());
                        String hostname = url.getHost() + ":" + url.getPort();
                        if (webhdfs.equals(hostname) || swebhdfs.equals(hostname)) {
                            return true;
                        }
                    } catch (MalformedURLException e) {
                        LOGGER.debug(e.getMessage());
                    }
                }
            }
        }
        return false;
    }
}
