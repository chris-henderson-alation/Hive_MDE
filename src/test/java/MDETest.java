import MDE.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class MDETest {

    public static final String site;
    public static final String core;

    static {
        try {
            FileReader r = new FileReader("/Users/chris.henderson/hack/Hive_MDE/configs/hive-site.xml");
            site = IOUtils.toString(r);
            r = new FileReader("/Users/chris.henderson/hack/Hive_MDE/configs/core-site.xml");
            core = IOUtils.toString(r);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testConstruction() throws Exception {
//        MetadataExtraction mde = new MetadataExtraction("mduser", "hyp3rbAd", IOUtils.toInputStream(site), IOUtils.toInputStream(core));
//        for (String database : mde.metastore.getDatabases("*")) {
//            for (String table : mde.metastore.getAllTables(database)) {
//                System.out.println(mde.metastore.getSchema(database, table));
////                System.out.println(mde.metastore.getTable(database, table));
//            }
//            System.out.println(mde.metastore.getTableObjectsByName(database, mde.metastore.getAllTables(database)));
//        }
//        System.out.println(mde.metastore.getTable("default", "commented").getParameters().get("comment"));
    }

    private static final DateFormat iso_date_format = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm'Z'");

    @Test
    public void testCSV() throws Exception {
        Appendable out = new StringWriter();
        CSVPrinter csv = CSVFormat.DEFAULT.print(out);
        csv.printRecord(Arrays.asList(new String[]{"asd", "asd"}));
        System.out.println(csv.getOut().toString());
    }

    @Test
    public void trace() throws Exception {
        JSONObject tableJson = new JSONObject();

        JSONObject skew = new JSONObject();
        JSONArray skew_cols = new JSONArray();
        JSONArray skew_val_tuples = new JSONArray();


        skew_cols.addAll(Arrays.asList());
        for (List<String> tuple : Arrays.asList(new ArrayList<String>())) {
            JSONArray skew_tuple = new JSONArray();
            skew_tuple.addAll(tuple);
            skew_val_tuples.add(skew_tuple);
        }
        skew.put("columns", skew_cols);
        skew.put("values", skew_val_tuples);

        tableJson.put("skews", skew);
        System.out.println(tableJson.toJSONString());
    }

    @Test
    public void dontlookatit() throws Exception {
        TableInfo t = new TableInfo();
        StringWriter w = new StringWriter();
        new ObjectMapper().writeValue(w, t);
        System.out.println(w.toString());
    }

    @Test
    public void hashcheck() throws Exception {
        Set<String> set = new HashSet<>(null);
    }


    @Test
    public void soupToNuts() throws Exception {
        SchemaFilter filter = new SchemaFilter(SchemaFilter.NO_RESTRICTIONS, SchemaFilter.NO_RESTRICTIONS);
        StringWriter writer = new StringWriter();
        MetadataCollector collector = new MetadataCollector(writer);
        HiveMetaStoreClient metastore = HiveMetastore.connect("mduser", "hyp3rbAd", IOUtils.toInputStream(site), IOUtils.toInputStream(core));
        MetadataExtraction mde = new MetadataExtraction(metastore, collector, filter);
        mde.extract();
        System.out.println(writer.toString());
    }

    @Test
    public void jakesHuge() throws Exception {
        long startTime = System.nanoTime();
        SchemaFilter filter = new SchemaFilter(SchemaFilter.NO_RESTRICTIONS, SchemaFilter.NO_RESTRICTIONS);
        MetadataCollector collector = new MetadataCollector(getOut());
        HiveMetaStoreClient metastore = HiveMetastore.connect("mduser", "hyp3rbAd",gatherConfigs("/Users/chris.henderson/hack/Hive_MDE/big"));
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

    public static InputStream[] gatherConfigs(String dir) throws Exception {
        File directory = new File(dir);
        ArrayList<InputStream> streams = new ArrayList<>();
        for (File file : directory.listFiles()) {
            if (file.getName().endsWith(".xml")) {
                streams.add(new FileInputStream(file));
            }
        }
        return streams.toArray(new InputStream[]{});
    }

    public static Writer getOut() throws Exception {
        return new FileWriter(new File("/tmp/out"));
    }
}
