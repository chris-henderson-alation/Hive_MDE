import org.apache.commons.cli.*;
import org.junit.Test;

public class maintest {

    @Test
    public void thing() throws Exception {

        Options options = new Options();

        Option input = new Option("i", "input", true, "input file path");
        input.setRequired(true);
        options.addOption(input);

        Option output = new Option("o", "output", true, "output file");
        output.setRequired(true);
        options.addOption(output);

        Option mde = new Option("m", "mde", false, "mde duh");
        mde.setRequired(true);
        options.addOption(mde);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, new String[]{"-i asd", "-o balss", "-m"});
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            return;
        }

        String inputFilePath = cmd.getOptionValue("input");
        String outputFilePath = cmd.getOptionValue("output");

        System.out.println(cmd.hasOption("m"));

        System.out.println(inputFilePath);
        System.out.println(outputFilePath);
    }

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
        options = new Options()
                .addOption(CONFIG_DIRECTORY, true, "configuration directory")
                .addOption(OUT, true, "output file, default is stdout")
                .addOptionGroup(mode)
                .addOption("u", true, "username")
                .addOption("p", true, "password");
    }

    @Test
    public void thiiiing() throws Exception {
        CommandLineParser cli = new DefaultParser();
        CommandLine cmd = cli.parse(options, new String[]{"--mde"});
        System.out.println(cmd.hasOption("m"));
    }

}
