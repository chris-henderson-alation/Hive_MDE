package Configuration;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;
import org.apache.xerces.jaxp.SAXParserFactoryImpl;
import org.unix4j.Unix4j;
import org.unix4j.unix.Find;
import org.unix4j.unix.Grep;
import org.unix4j.unix.find.FindOptions;
import org.unix4j.unix.grep.GrepOption;
import org.unix4j.unix.grep.GrepOptions;
import org.unix4j.unix.xargs.XargsOption;
import org.unix4j.unix.xargs.XargsOptions;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


import javax.xml.parsers.*;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Configuration {

    private static final Logger LOGGER = Logger.getLogger(Configuration.class.getName());



    private static final String FIND_HIVE_CONFS = "find %s -name *xml | xargs grep -l \"<configuration>\"";
    private static final String FIND_KNOX_TOPOLOGIES = "find %s -name *xml | xargs grep -l \"<toplogy>\"";
    private static final String FIND_KRB5 = "find %s -name krb5.conf";
    private static final String FIND_JAVA_KEYSTORES = "find %s -name *jks";

    private final HiveConf hiveConf = new HiveConf();
    private final HashMap<String, Document> topologies = new HashMap<>();
    private String krb5 = "";

    public Configuration(String directory) {
        this.gather(directory);
    }

    public String something() {
        String http = this.hiveConf.get("dfs.namenode.http-address");
        String https = this.hiveConf.get("dfs.namenode.https-address");
        for (String cluster : this.topologies.keySet()) {
            Document topology = this.topologies.get(cluster);
            NodeList services = topology.getElementsByTagName("service");
            for (int i = 0; i < services.getLength(); i++) {
                Node service = services.item(i);
                NodeList children = service.getChildNodes();
                for (int j = 0; j < children.getLength(); j++) {
                    Node child = children.item(j);
                    if ("url".equals(child.getNodeName())) {
                        try {
                            URL url = new URL(child.getTextContent());
                            String hostname = url.getHost() + ":" + url.getPort();
                            if (http.equals(hostname) || https.equals(hostname)) {
                                return cluster;
                            }
                        } catch (MalformedURLException e) {
                            LOGGER.debug(e.getMessage());
                            continue;
                        }
                    }
                }
            }
        }
        throw new RuntimeException("tantrums");
    }

    private void gather(String directory) {
        this.gatherHiveConfs(directory);
        this.gatherTopologies(directory);
        this.gatherKerberosConfs(directory);
    }

    private void gatherHiveConfs(String directory) {
        List<String> hiveConfs = findPipeXargsGrep(directory, "*xml", "<configuration>");
        for (InputStream hiveConf : openAll(hiveConfs.toArray(new String[]{}))) {
            this.hiveConf.addResource(hiveConf);
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

    private static List<String> find(String directory, String pattern) {
        return Unix4j.find(directory, pattern).toStringList();
    }

    private static List<String> findPipeXargsGrep(String directory, String pattern, String grepPattern) {
        return Unix4j.grep(Grep.Options.l, grepPattern, find(directory, pattern).toArray(new String[]{})).toStringList();
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

    public HiveConf HiveConf() {
        if (this.hiveConf != null) {
            return this.hiveConf;
        }
        return null;
    }
}
