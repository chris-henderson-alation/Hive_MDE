import MDE.HiveMetastore;
import MDE.MetadataCollector;
import MDE.MetadataExtraction;
import MDE.SchemaFilter;
import QLI.HDFS;
import QLI.HDFSClient;
import QLI.QuerLogIngestion;
import org.apache.commons.cli.*;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import sun.rmi.runtime.Log;

import javax.security.auth.login.LoginException;
import java.io.*;
import java.util.ArrayList;

public class command {

    private enum Mode {
        MDE,
        QLI
    }

    private static final Options options;
    private static final String MDE = "m";
    private static final String QLI = "q";
    private static final String OUT = "o";
    private static final String CONFIG_DIRECTORY = "d";

    static {
        OptionGroup mode = new OptionGroup()
                .addOption(new Option(MDE, "mde", false,"execute Metadata Extraction"))
                .addOption(new Option(QLI, "qli", false, "execute Querylog Ingestion"));
        mode.setRequired(true);
        Option dir = new Option(CONFIG_DIRECTORY, true, "configuration directory");
        dir.setRequired(true);
        options = new Options()
            .addOption(dir)
            .addOption(OUT, true, "output file, default is stdout")
            .addOptionGroup(mode)
            .addOption("u", true, "username")
            .addOption("p", true, "password");
    }

    public static void main(String ... argv) {
        CommandLine opts = parseCLI(argv);
        Mode mode = mode(opts);
        Writer out = out(opts);
        InputStream[] configurations = configs(opts);
        String username = opts.getOptionValue("u");
        String password = opts.getOptionValue("p");
        run(mode, out, configurations, username, password);
    }

    public static void run(Mode mode, Writer out, InputStream[] configurations, String username, String password) {
        switch (mode) {
            case MDE:
                mde(out, configurations, username, password);
                break;
            case QLI:
                qli(out, configurations, username, password);
                break;
        }
    }

    public static void qli(Writer out, InputStream[] configurations, String username, String password) {
        HDFSClient client;
        try {
            client = new HDFS(username, password, configurations);
        } catch (LoginException | IOException | InterruptedException e) {
            System.out.println("don't feel like it yet");
            System.out.println(e.getMessage());
            System.exit(1);
            return;
        }
        QuerLogIngestion qli = new QuerLogIngestion(client);
        qli.out = out;
        qli.search();
    }

    public static void mde(Writer out, InputStream[] configurations, String username, String password) {
        SchemaFilter filter = new SchemaFilter(SchemaFilter.NO_RESTRICTIONS, SchemaFilter.NO_RESTRICTIONS);
        MetadataCollector collector;
        HiveMetaStoreClient metastore;
        try {
            collector = new MetadataCollector(out);
        } catch (IOException e) {
            System.out.println("failed to begin writing to the out stream for MDE");
            System.out.println(e.getMessage());
            System.exit(1);
            return;
        }
        try {
            metastore = HiveMetastore.connect(username, password ,configurations);
        } catch (MetaException | IOException e) {
            System.out.println("failed to initialize connection with the metastore");
            System.out.println(e.getMessage());
            System.exit(1);
            return;
        } catch (LoginException e) {
            System.out.println("failed to login");
            System.out.println(e.getMessage());
            System.exit(1);
            return;
        }
        MetadataExtraction mde = new MetadataExtraction(metastore, collector, filter);
        mde.extract();
    }

    public static CommandLine parseCLI(String ... argv) {
        DefaultParser parser = new DefaultParser();
        try {
            return parser.parse(options, argv);
        }
        catch (ParseException e) {
            System.out.println(e.getMessage());
            System.exit(1);
            return null;
        }
    }

    public static Mode mode(CommandLine opts) {
        if (opts.hasOption(MDE)) {
            return Mode.MDE;
        }
        if (opts.hasOption(QLI)) {
            return Mode.QLI;
        }
        throw new RuntimeException("dead code");
    }

    public static Writer out(CommandLine opts) {
        if (!opts.hasOption(OUT)) {
            return new PrintWriter(System.out);
        }
        File file = new File(opts.getOptionValue(OUT));
        if (file.isDirectory()) {
            System.out.println(file.getAbsolutePath() + " is a directory");
            System.exit(1);
        }
        boolean alreadyExisted = false;
        try {
            alreadyExisted = file.createNewFile();
        } catch (IOException e){
            System.out.println("failed to create the out file at " + file.getAbsolutePath());
            System.out.println(e.getMessage());
            System.exit(1);
        }
        if (alreadyExisted && !file.canWrite()) {
            System.out.println(file.getAbsolutePath() + " is not writable");
            System.exit(1);
        }
        try {
            return new FileWriter(file);
        } catch (IOException e) {
            System.out.println("failed to open " + file.getAbsolutePath() + " for writing");
            System.out.println(e.getMessage());
            System.exit(1);
            // javac cannot tell that this is dead code.
            return null;
        }
    }

    public static InputStream[] configs(CommandLine opts) {
        if (!opts.hasOption(CONFIG_DIRECTORY)) {
            System.out.println("configuration directory must be set");
            System.exit(1);
        }
        String dir = opts.getOptionValue(CONFIG_DIRECTORY);
        File directory = new File(dir);
        ArrayList<InputStream> streams = new ArrayList<>();
        for (File file : directory.listFiles()) {
            if (file.getName().endsWith(".xml")) {
                try {
                    streams.add(new FileInputStream(file));
                } catch (IOException e){
                    System.out.println("failed to open the config at " + file.getAbsolutePath());
                    System.out.println(e.getMessage());
                    System.exit(1);
                }
            }
        }
        return streams.toArray(new InputStream[]{});
    }
}
