package MDE;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;


public class MetadataExtraction {

    public static final int MAX_TABLE_QUERY_SIZE = 10000;

    public HiveMetaStoreClient metastore;
    public MetadataCollector collector;
    public SchemaFilter filter;

    static {
        Logger.getRootLogger().setLevel(Level.OFF);
    }

    public MetadataExtraction(HiveMetaStoreClient metastore, MetadataCollector collector, SchemaFilter filter) {
        this.metastore = metastore;
        this.collector = collector;
        this.filter = filter;
    }

    public void extract() {
        try {
            this.metastore.getDatabases("*")
                    .stream()
                    .filter(this.filter::accepted)
                    .forEach(this::extractDatabase);
        } catch (MetaException e) {
            this.collector.fatalError(e);
        } finally {
            this.collector.finish();
        }
    }

    private void extractDatabase(String database) {
        try {
            this.extractTables(database, this.metastore.getAllTables(database));
        } catch (MetaException e) {
            this.collector.fatalError(e);
        }

    }

    private void extractTables(String database, List<String> tables) {
        try {
            this.metastore.getTableObjectsByName(database, tables)
                    .stream()
                    .parallel()
                    .forEach(this::extractTable);
        } catch (Exception e) {
            this.collector.fatalError(e);
        }
    }

    private void extractTable(Table table) {
        this.collector.collect(new TableInfo(table));
    }



    public static <T> List<List<T>> chunk(List<T> tables) {
        List<List<T>> t = new ArrayList<>();
        int start = 0;
        int end = tables.size() > MAX_TABLE_QUERY_SIZE ? MAX_TABLE_QUERY_SIZE : tables.size();
        t.add(tables.subList(start, end));
        while (end < tables.size()) {
            start = end;
            end = tables.size() > start + MAX_TABLE_QUERY_SIZE ? start + MAX_TABLE_QUERY_SIZE : tables.size();
            t.add(tables.subList(start, end));
        }
        return t;
    }
}
