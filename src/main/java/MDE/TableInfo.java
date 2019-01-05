package MDE;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.hive.metastore.api.*;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

@JsonSerialize(using = TableInfo.Serializer.class)
public class TableInfo {

    public String name;
    public String schema;
    public String type;
    public String remarks;
    public String definitionSQL;
    public String viewExpandedText;
    public String viewOriginalText;
    public String hiveTableType;
    public String owner;
    public Date createTime;
    public Date alterTime;
    public Long peakSpaceBytes = 0L;
    public Long currentSpaceBytes = 0L;
    public List<String> partitionKeys = new ArrayList<>();
    public String dataLocation;
    public String serdeName;
    public String serdeLib;
    public List<String> bucketKeys = new ArrayList<>();
    public List<String> sortKeys = new ArrayList<>();
    public List<String> skewColumns;
    public List<List<String>> skewValueTuples;

    public TableInfo(Table table) {
        this.name = table.getTableName();
        this.schema = table.getDbName();
        this.type = table.getTableType();
        this.remarks = table.getParameters().get("comment");
        this.viewExpandedText = table.getViewExpandedText();
        this.viewOriginalText = table.getViewOriginalText();
        this.hiveTableType = normalizeType(table.getTableType());
        this.owner = table.getOwner();
        this.createTime = new Date(1000L * table.getCreateTime());
        this.partitionKeys = table.getPartitionKeys().stream().parallel()
                .map(FieldSchema::getName)
                .collect(Collectors.toList());
        this.serdeName = table.getSd().getSerdeInfo().getName();
        this.serdeLib = table.getSd().getSerdeInfo().getSerializationLib();
        this.skewColumns = table.getSd().getSkewedInfo().getSkewedColNames();
        this.skewValueTuples = table.getSd().getSkewedInfo().getSkewedColValues();
        this.dataLocation = table.getSd().getLocation();
        this.bucketKeys = table.getSd().getBucketCols();
        this.sortKeys = table.getSd().getSortCols().stream().parallel()
                .map(key -> key.getOrder() < 0 ? "-" : "" + key.getCol())
                .collect(Collectors.toList());
    }

    public TableInfo() {
    }

    private static String normalizeType(String tableType) {
        if (tableType == null) {
            return "TABLE";
        } else if (tableType.contains("TABLE")) {
            return "TABLE";
        } else if (tableType.contains("VIEW")) {
            return "VIEW";
        } else {
            return tableType;
        }
    }

    public static class Serializer extends JsonSerializer<TableInfo> {

        private static final DateFormat iso_date_format = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm'Z'");

        @Override
        public void serialize(TableInfo table, org.codehaus.jackson.JsonGenerator jsonGenerator, org.codehaus.jackson.map.SerializerProvider provider) throws IOException, JsonProcessingException {
            CSVPrinter csv;
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("name", table.name);
            jsonGenerator.writeStringField("schema", table.schema);
            jsonGenerator.writeStringField("type", table.type);
            jsonGenerator.writeStringField("remark", table.remarks);
            jsonGenerator.writeStringField("definition_sql", table.definitionSQL);
            jsonGenerator.writeStringField("view_expanded_text", table.viewExpandedText);
            jsonGenerator.writeStringField("view_original_text", table.viewOriginalText);
            jsonGenerator.writeStringField("table_owner", table.owner);
            jsonGenerator.writeStringField("table_type", table.hiveTableType);
            jsonGenerator.writeStringField("table_create_time", table.createTime == null ? null : iso_date_format.format(table.createTime));
            jsonGenerator.writeStringField("table_alter_time", table.alterTime == null ? null : iso_date_format.format(table.alterTime));
            jsonGenerator.writeNumberField("peak_space_bytes", table.peakSpaceBytes);
            jsonGenerator.writeNumberField("current_space_bytes", table.currentSpaceBytes);
            jsonGenerator.writeStringField("data_location", table.dataLocation);
            jsonGenerator.writeStringField("serde_name", table.serdeName);
            jsonGenerator.writeStringField("serde_lib", table.serdeLib);

            csv = CSVFormat.DEFAULT.print(new StringWriter());
            csv.printRecord(table.partitionKeys);
            jsonGenerator.writeStringField("partition_keys_csv", csv.getOut().toString().trim());

            csv = CSVFormat.DEFAULT.print(new StringWriter());
            csv.printRecord(table.bucketKeys);
            jsonGenerator.writeStringField("bucket_keys_csv", csv.getOut().toString().trim());

            csv = CSVFormat.RFC4180.print(new StringWriter());
            csv.printRecord(table.sortKeys);
            jsonGenerator.writeStringField("sort_keys_csv", csv.getOut().toString().trim());

            // "skews":{"columns":[],"values":[[]]}
            jsonGenerator.writeObjectFieldStart("skews");
            jsonGenerator.writeObjectField("columns", table.skewColumns);
            jsonGenerator.writeObjectField("values", table.skewValueTuples);
            jsonGenerator.writeEndObject();

            jsonGenerator.writeEndObject();
        }
    }


    @Override
    public String toString() {
        return "TableInfo{" +
                "name='" + name + '\'' +
                ", schema='" + schema + '\'' +
                ", type='" + type + '\'' +
                ", remarks='" + remarks + '\'' +
                ", definitionSQL='" + definitionSQL + '\'' +
                ", viewExpandedText='" + viewExpandedText + '\'' +
                ", viewOriginalText='" + viewOriginalText + '\'' +
                ", hiveTableType='" + hiveTableType + '\'' +
                ", owner='" + owner + '\'' +
                ", createTime=" + createTime +
                ", alterTime=" + alterTime +
                ", peakSpaceBytes=" + peakSpaceBytes +
                ", currentSpaceBytes=" + currentSpaceBytes +
                ", partitionKeys=" + partitionKeys +
                ", dataLocation='" + dataLocation + '\'' +
                ", serdeName='" + serdeName + '\'' +
                ", serdeLib='" + serdeLib + '\'' +
                ", bucketKeys=" + bucketKeys +
                ", sortKeys=" + sortKeys +
                ", skewColumns=" + skewColumns +
                ", skewValueTuples=" + skewValueTuples +
                '}';
    }

}
