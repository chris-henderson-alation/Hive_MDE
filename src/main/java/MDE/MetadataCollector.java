package MDE;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.ObjectCodec;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Writer;
import java.util.Vector;

public class MetadataCollector {

    public static final int MAX_MEMORY_USAGE = 10;

    private Vector<TableInfo> buffer = new Vector<>();
    private JsonGenerator jsonGenerator;
    private Writer writer;

    public MetadataCollector(Writer writer) throws IOException {
        this.writer = writer;
        this.jsonGenerator = new JsonFactory().createJsonGenerator(writer).setCodec(new ObjectMapper());
        this.jsonGenerator.writeStartArray();
    }

    public void collect(TableInfo table) {
        this.flushIfNecessary();
        this.buffer.add(table);
    }

    public void finish() {
        this.flush();
        try {
            this.jsonGenerator.writeEndArray();
            this.jsonGenerator.flush();
        } catch (Exception e) {
            this.fatalError(e);
        }
    }

    private synchronized void flushIfNecessary() {
        if (pastMemoryThreshold()) {
            this.flush();
        }
    }

    private void flush()  {
        this.buffer.stream().forEach(this::write); ;
        this.buffer = new Vector<>();
        Runtime.getRuntime().gc();
    }

    private void write(TableInfo table) {
        try {
            this.jsonGenerator.writeObject(table);
        } catch (Exception e) {
            this.fatalError(e);
        }
    }

    public void nonFatalError(Exception e) {
        System.out.println(ExceptionUtils.getFullStackTrace(e));
    }

    public void fatalError(Exception e) {
        System.out.println(ExceptionUtils.getFullStackTrace(e));
    }

    public static boolean pastMemoryThreshold() {
        return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / Runtime.getRuntime().totalMemory() * 100 >= MAX_MEMORY_USAGE;
    }

}
