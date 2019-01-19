import MDE.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;

import java.io.*;
import java.util.*;

public class MDETest {

    public static final String MDUSER = "mduser";
    public static final String HYP3RBAD = "hyp3rbAd";


    @Test
    public void jakesHuge() throws Exception {
        long startTime = System.nanoTime();
        SchemaFilter filter = new SchemaFilter(SchemaFilter.NO_RESTRICTIONS, SchemaFilter.NO_RESTRICTIONS);
        MetadataCollector collector = new MetadataCollector(getOut());
        HiveMetaStoreClient metastore = HiveMetastore.connect("hive", "/Users/chris.henderson/hack/Hive_MDE/big");
        MetadataExtraction mde = new MetadataExtraction(metastore, collector, filter);
        mde.extract();
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        System.out.println(duration / 1e9);
    }

    @Test
    public void testChunk() {
        List<Integer> data = Arrays.asList(1,2,3,4,5,6,7);
        System.out.println(MetadataExtraction.chunk(data));
    }

    @Test
    public void angry() throws Exception {
        SchemaFilter filter = new SchemaFilter(SchemaFilter.NO_RESTRICTIONS, SchemaFilter.NO_RESTRICTIONS);
        Writer out = getOut();
        MetadataCollector collector = new MetadataCollector(out);
        HiveMetaStoreClient metastore = HiveMetastore.connect("hive",   "/Users/chris.henderson/hack/Hive_MDE/matrix/knox");
        MetadataExtraction mde = new MetadataExtraction(metastore, collector, filter);
        mde.extract();
        System.out.println(out.toString());
    }

    @Test
    public void superAngry() throws Exception {
        SchemaFilter filter = new SchemaFilter(SchemaFilter.NO_RESTRICTIONS, SchemaFilter.NO_RESTRICTIONS);
//        Writer out = getOut();
        Writer out = new StringWriter();
        MetadataCollector collector = new MetadataCollector(out);
        HiveMetaStoreClient metastore = HiveMetastore.connect("mduser", "hyp3rbAd", "/Users/chris.henderson/hack/Hive_MDE/matrix/knoxKerberos");
        MetadataExtraction mde = new MetadataExtraction(metastore, collector, filter);
        mde.extract();
        System.out.println(out.toString());
    }

    @Test
    public void knox() throws Exception {
        SchemaFilter filter = new SchemaFilter(SchemaFilter.NO_RESTRICTIONS, SchemaFilter.NO_RESTRICTIONS);
        Writer out = new StringWriter();
        MetadataCollector collector = new MetadataCollector(out);
        HiveMetaStoreClient metastore = HiveMetastore.connect("mduser", "hyp3rbAd", "/Users/chris.henderson/hack/Hive_MDE/matrix/jakes");
        MetadataExtraction mde = new MetadataExtraction(metastore, collector, filter);
        mde.extract();
        System.out.println(out.toString());
    }

    @Test
    public void azure() throws Exception {
        SchemaFilter filter = new SchemaFilter(SchemaFilter.NO_RESTRICTIONS, SchemaFilter.NO_RESTRICTIONS);
        Writer out = new StringWriter();
        MetadataCollector collector = new MetadataCollector(out);
        HiveMetaStoreClient metastore = HiveMetastore.connect("/Users/chris.henderson/hack/Hive_MDE/matrix/azure");
        MetadataExtraction mde = new MetadataExtraction(metastore, collector, filter);
        mde.extract();
        System.out.println(out.toString());
    }

    public static InputStream[] gatherConfigs(String dir) throws Exception {
        return command.configs(dir);
    }

    public static Writer getOut() throws Exception {
        return new FileWriter(new File("/tmp/out"));
    }
}
