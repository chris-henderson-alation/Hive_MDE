import Configuration.AlationHiveConfiguration;
import MDE.HiveMetastore;
import MDE.MetadataCollector;
import MDE.MetadataExtraction;
import MDE.SchemaFilter;
import QLI.QueryLogIngestion;
import QLI.client.Client;
import QLI.client.ClientFactory;
import kerberos.Kerberos;
import org.apache.commons.cli.*;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.security.auth.login.LoginException;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;

public class command {

    private static final Logger LOGGER = Logger.getLogger(command.class.getName());

    private enum Mode {
        MDE,
        QLI
    }

    private static final Options options;
    private static final String MDE = "m";
    private static final String MDE_LONG = "mde";
    private static final String QLI = "q";
    private static final String QLI_LONG = "qli";
    private static final String OUT = "o";
    private static final String KNOX = "k";
    private static final String KNOX_LONG = "knox";
    private static final String CONFIG_DIRECTORY = "d";
    private static final String VERBOSE = "v";
    private static final String VERY_VERBOSE = "vv";
    private static final String VERY_VERY_VERBOSE = "vvv";

    private static final String ERROR_RENDEVOUS = "e";
    private static final String ERROR_RENDEVOUS_LONG = "error";

    static {
        OptionGroup mode = new OptionGroup()
                .addOption(new Option(MDE, MDE_LONG, false,"execute Metadata Extraction"))
                .addOption(new Option(QLI, QLI_LONG, false, "execute Querylog Ingestion"));
        OptionGroup verbosity = new OptionGroup()
                .addOption(new Option(VERBOSE, false, "verbose logging"))
                .addOption(new Option(VERY_VERBOSE, false, "very verbose logging"))
                .addOption(new Option(VERY_VERY_VERBOSE, false, "very very verbose logging"));;
        Option configDirectory = new Option(CONFIG_DIRECTORY, true, "configuration directory");
        Option out = new Option(OUT, true, "output file, default is stdout");
        Option knox = new Option(KNOX, KNOX_LONG, true, "hostname of the Knox proxy");
        Option username = new Option("u", true, "username");
        Option password= new Option("p", true, "password");

        configDirectory.setRequired(true);
        mode.setRequired(true);

        options = new Options()
                .addOptionGroup(mode)
                .addOptionGroup(verbosity)
                .addOption(configDirectory)
                .addOption(out)
                .addOption(knox)
                .addOption(username)
                .addOption(password);
    }

    public static void main(String ... argv) throws Exception {

        CommandLine opts = parseCLI(argv);
        verbosity(opts);
        Mode mode = mode(opts);
        Writer out = out(opts);
        AlationHiveConfiguration configuration = new AlationHiveConfiguration(opts.getOptionValue(CONFIG_DIRECTORY));
        String username = opts.getOptionValue("u");
        String password = opts.getOptionValue("p");
        String knoxHostname = opts.getOptionValue(KNOX);
        run(mode, out, configuration, username, password, knoxHostname);
    }

    public static void run(Mode mode, Writer out, AlationHiveConfiguration configurations, String username, String password, String knoxHost) throws Exception {
        switch (mode) {
            case MDE:
                mde(out, configurations, username, password, knoxHost);
                break;
            case QLI:
                qli(out, configurations, username, password, knoxHost);
                break;
        }
    }

    public static void qli(Writer out, AlationHiveConfiguration configurations, String username, String password, String knoxHost) throws Exception {
        Client client;
        try {
            client = ClientFactory.newClient(configurations, username, password, knoxHost);
        } catch (LoginException | IOException | InterruptedException e) {
            System.out.println("don't feel like it yet");
            System.out.println(e.getMessage());
            System.exit(1);
            return;
        }
        QueryLogIngestion qli = new QueryLogIngestion(client);
        qli.out = out;
        qli.search();
        for (Exception e : qli.ioExceptions) {
            System.out.println(e.getMessage());
        }
        for (Exception e : qli.remoteExceptions) {
            System.out.println(e.getMessage());
        }
        if (qli.ioExceptions.size() != 0 || qli.remoteExceptions.size() != 0) {
            System.exit(1);
        }
    }

    public static void mde(Writer out, AlationHiveConfiguration configurations, String username, String password, String knoxHostname) {
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
            if (password != null) {
                LOGGER.info("initializing kerberos");
                metastore = HiveMetastore.connect(configurations, username, password);
            } else if (username != null) {
                metastore = HiveMetastore.connect(configurations, username);
            } else {
                metastore = HiveMetastore.connect(configurations);
            }
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

    public static Writer errorOut(CommandLine opts) {
        return null;
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

    public static InputStream[] configs(String dir) {
        ArrayList<InputStream> streams = new ArrayList<>();
        buildConfigs(streams, dir);
        return streams.toArray(new InputStream[]{});
    }

    public static void buildConfigs(ArrayList<InputStream> streams, String dir) {
        File directory = new File(dir);
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) {
                buildConfigs(streams, file.getAbsolutePath());
                continue;
            }
            if (file.getName().endsWith(".xml")) {
                try {
                    streams.add(new FileInputStream(file));
                    LOGGER.info("added config file at " + file.toString());
                    continue;
                } catch (IOException e) {
                    System.out.println("failed to open the config at " + file.getAbsolutePath());
                    System.out.println(e.getMessage());
                    System.exit(1);
                }
            }
            if (Kerberos.KRB5_CONF.equals(file.getName())) {
                Kerberos.setKrb5Conf(file.getAbsolutePath());
            }
        }
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

    public static void verbosity(CommandLine opts) {
        if (opts.hasOption(VERBOSE)) {
            Logger.getRootLogger().setLevel(Level.INFO);
        } else if (opts.hasOption(VERY_VERBOSE)) {
            Logger.getRootLogger().setLevel(Level.DEBUG);
        } else if (opts.hasOption((VERY_VERY_VERBOSE))) {
            Logger.getRootLogger().setLevel(Level.TRACE);
        } else {
            Logger.getRootLogger().setLevel(Level.ERROR);
        }
    }
}
