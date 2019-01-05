package MDE;

import kerberos.Kerberos;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;


public class MetadataExtraction {

    public HiveMetaStoreClient metastore;
    public MetadataCollector collector;
    public SchemaFilter filter;

    public MetadataExtraction(HiveMetaStoreClient metastore, MetadataCollector collector, SchemaFilter filter) {
        this.metastore = metastore;
        this.collector = collector;
        this.filter = filter;
    }

    public void extract() {
        try {
            this.metastore.getDatabases("*")
                    .stream()
                    .parallel()
                    .forEach(this::extractDatabase);
        } catch (MetaException e) {
            this.collector.fatalError(e);
        } finally {
            this.collector.finish();
        }
    }

    private void extractDatabase(String database) {
        try {
            this.extracTables(database, this.metastore.getAllTables(database));
        } catch (MetaException e) {
            this.collector.fatalError(e);
        }

    }

    private void extracTables(String database, List<String> tables) {
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
}
