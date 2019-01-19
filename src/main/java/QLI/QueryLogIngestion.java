package QLI;

// Please see ./org/apache/hadoop/README.md for a quick explanation of these packages.
import QLI.client.Client;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.jobhistory.*;
import org.apache.hadoop.mapreduce.Counters;

import com.google.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * QLI implements query log ingestion for Hive datasources. Hive is unique among our target datasources in that it is
 * is ultimately resolvable to files on a flat filesystem (or, at least, an abstraction such as the Hadoop Filesystem).
 * As such, QLI for Hive is implemented as a depth-first search through a filesystem (see {@link Searcher} for the
 * DFS and backtracking algorithm).
 *
 * Query Log Ingestion is a (possibly) large batch job, with potential for many failures and many successes
 * within a single run of QLI. As such, we would like to perform "best effort" completion of each run of QLI,
 * accumulating as many successes as possible. If any errors occur during QLI, then they are recorded in
 * their respective vector and the crawler continues. After execution, these vectors may be accessed and
 * reviewed for any errors that may have occurred. See {@link #remoteExceptions}, {@link } and
 * {@link #ioExceptions} for their descriptions.
 *
 * Example usages:
 *  QLI qli = new QLI(hadoopRestClient, 0, 100000000, null, 8);
 *  qli.search("/mr-history/done");
 *  System.out.println(qli.QLI);
 *  System.out.println(qli.remoteExceptions);
 *  System.out.println(qli.apiExceptions);
 *  System.out.println(qli.ioExceptions);
 */
public class QueryLogIngestion extends Searcher<FileStatus> {

    private static final Logger LOGGER = Logger.getLogger(QueryLogIngestion.class.getName());

    /**
     * remoteExceptions is a collection of all of the RemoteExcepctions that occurred during QLI.
     *
     * A {@link } indicates that there was some sort of error communicating with the remote HDFS and
     * are resolvable to the errors enumerated https://hadoop.apache.org/docs/r1.0.4/webhdfs.html#HTTP+Response+Codes.
     *
     * Some examples of RemoteException:
     *  - Filesystem permissions.
     *  - File not found.
     *  - 500.
     */
    public final Vector<Exception> remoteExceptions = new Vector<>();

    /**
     * ioExceptions is a collection of all of the IOExcepctions that occurred during QLI.
     *
     * The presence of errors within this collection should be viewed with great concern as it is used mostly
     * to be thorough about handling possible IOExceptions that QLI's dependencies may throw. However, by now all
     * contents of files should be in local memory, so this vector really should never be non-empty.
     */
    public final Vector<IOException> ioExceptions = new Vector<>();

    /**
     * QLI is the final record of all query QLI discovered by {@link #search(String)}
     */
    public final Vector<QueryLog> logs = new Vector<>();

    /**
     * A table to keep track of discovered query QLI by their JobID.
     *
     * A job configuration and a job history reside in separate files, a .xml and a .jhist (respectively).
     * Since we are not guaranteed to find one or the other first, and we are downloading files in parallel, we need
     * a way for individual threads to see if the associated QueryLog has already been created due to discover of a
     * sister conf/history file.
     *
     * A pointer to each QueryLog is kept in {@link #logs} as that is the data structure intended for final
     * consumption by callers.
     */
    private final ConcurrentHashMap<JobID, QueryLog> logTable = new ConcurrentHashMap<>();

    /**
     * The maximum number of QLI to ingest. A value -1 signifies unlimited log ingestion.
     */
    private final long limit;

    /**
     * A user provided regular expression. This regular expression will be applied to the filename of each query log
     * discovered. If the regular expression matches the filename, then that log will be ignored.
     *
     * This regular expression does not apply to directories. Therefor, given the regular expression "nope.*" and
     * the following directory tree:
     *
     * nope
     * ├── nope.txt
     * └── yup.txt
     *
     * nope.txt will be ignored but yup.txt will be inspected.
     */
    private final Pattern filenameFilter;

    /**
     * A dependency injection of the Hadoop client to use for QLI. Implementors of this interface must be able to
     * accurately describe the GETFILESTATUS and LISTSTATUS
     */
    private final Client client;

    /**
     * Querying HDFS for directory listings isn't so bad (although we would really like a "tree" like API to speed things up),
     * however downloading job configurations and history files can be large, so we use a thread pool to conduct those
     * larger file downloads in parallel.
     */
    private final ExecutorService pool;

    /**
     * A ThreadPoolExecutor is not particularly well suited for ad-hoc work - rather the assumption is for long
     * lasting work. The boilerplate example of "how to wait for a ThreadPool" is to call pool.shutdown()
     * followed by pool.awaitTermination(). This is incorrect for our purposes. pool.shutdown() rejects ALL further
     * work submitted to the pool. This means that if the main discovery thread finishes while work is enqueued then
     * that work will be summarily dropped.
     *
     * Instead, are using the Phaser class, which is similar to Golang's WaitGroup.
     *
     * Before creation of each thread one "registers" with the Phaser. Upon completion, the thread "arrives".
     * This is equivalent to incrementing and decrementing an atomic value by one, respectively.
     *
     * "waiting" for the Phaser effective waits for this atomic value to reach zero.
     */
    private final Phaser waitGroup;

    /**
     * An assumption of ours is that all job histories end in the .jhist file extension.
     */
    public static final String HISTORY_FILE_SUFFIX = "jhist";

    /**
     * An assumption of ours is tha all job configurations end in .xml file extension.
     */
    public static final String CONFIGURATION_FILE_SUFFIX = "xml";

    /**
     * We are using the same regex used by MapR to generate these to discover files that contain Job IDs. The following
     * is in-code documentation from the Apache Hadoop project regarding the format of these strings:
     *
     *  https://github.com/apache/hadoop/blob/c96cbe8659587cfc114a96aab1be5cc85029fe44/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/JobID.java#L54
     *
     *  JobID represents the immutable and unique identifier for
     *  the job. JobID consists of two parts. First part
     *  represents the jobtracker identifier, so that jobID to jobtracker map
     *                  is defined. For cluster setup this string is the jobtracker
     *  start time, for local setting, it is "local" and a random number.
     *  Second part of the JobID is the job number. <br>
     *                  An example JobID is :
     *  <code>job_200707121733_0003</code> , which represents the third job
     *  running at the jobtracker started at <code>200707121733</code>.
     *  <p>
     *
     */
    private static final Pattern JOB_ID_REGEX;

    /**
     * Key to the jtIdentifier as a named capture group in {@link #JOB_ID_REGEX}.
     *
     * Short for "job tracker identifier". This is the first numerical component of a Hive JobID.
     * It may be tempting to use this as a timestamp for the job, because sometimes it is,
     * but as discussed in the comment for {@link #JOB_ID_REGEX} this is not always the case as it can somtetimes
     * just be a random number.
     */
    private static final String JTIDENTIFIER = "jtIdentifier";

    /**
     * Key to the id as a named capture group in {@link #JOB_ID_REGEX}.
     *
     * The "id" is the second numerical component to a Hive JobID.
     */
    private static final String ID = "id";

    /**
     * Key to the file extension as a named capture group in {@link #JOB_ID_REGEX}.
     */
    private static final String FILE_EXTENSION = "fileExtension";

    static {
        JOB_ID_REGEX = Pattern.compile(String.format("job_(?<%s>[0-9]+)_(?<%s>[0-9]+).*\\.(?<%s>jhist|xml)",
                JTIDENTIFIER, ID, FILE_EXTENSION));
    }

    /**
     * Used to retrieve metrics about the number of bytes written/read during this job.
     *
     * See {@link #parseJHIST(InputStream, FileStatus)} for usage.
     */
    private static final String FILESYSTEM_COUNTER_GROUPNAME = "org.apache.hadoop.mapreduce.FileSystemCounter";

    private enum State {
        ROOT,
        YEAR,
        MONTH,
        DAY,
        SECONDS,
        FILES
    }

    private State state = State.ROOT;

    private Date current;
    private Date earliest;
    private Date latest;

    public Writer out;

    /**
     *
     * @param client Dependency injection of the Hadoop client to use for QLI.
     * @param earliest Logs whose modification time is lower than this will be ignored.
     * @param latest Logs whose modification time is higher than this will be ignored.
     * @param limit The maximum number of query QLI to ingest. A value of -1 indicated unlimited.
     * @param filenameFilter A compile regular expression. Any querylog whose name matches this pattern will be ignored.
     * @param numThreads The maximum number of threads to use to conduct QLI.
     */
    public QueryLogIngestion(Client client, Calendar earliest, Calendar latest, int limit, Pattern filenameFilter, int numThreads) {
        this.pool = Executors.newFixedThreadPool(numThreads);
        this.waitGroup = new Phaser();
        this.client = client;
        this.earliest = earliest == null ? Date.MIN : new Date(earliest.get(Calendar.YEAR), earliest.get(Calendar.MONTH) + 1, earliest.get(Calendar.DAY_OF_MONTH));
        this.latest = latest == null ?  Date.MAX : new Date(latest.get(Calendar.YEAR), latest.get(Calendar.MONTH) + 1, latest.get(Calendar.DAY_OF_MONTH));
        this.limit = limit == -1 ? Long.MAX_VALUE : limit;
        this.filenameFilter = filenameFilter;
        this.state = State.ROOT;
        this.current = new Date();

        LOGGER.info("maximum thread count is " + numThreads);
        LOGGER.info("earliest acceptable timestamp is " + this.earliest.constructDateString());
        LOGGER.info("latest acceptable timestamp is " + this.latest.constructDateString());
        LOGGER.info("log limit is " + this.limit);
        LOGGER.info(this.filenameFilter == null ? "no filename filter provided" : "filename filter is " + this.filenameFilter.pattern());
    }

    /**
     * This convenience constructor defaults to allowing unlimited log consumption, no filename filtering,
     * and a max number of threads used equal to the number of available processors, as returned by the runtime.
     *
     * @param client
     * @param earliest
     * @param latest
     */
    public QueryLogIngestion(Client client, Calendar earliest, Calendar latest) {
        this(client, earliest, latest, -1, null, Runtime.getRuntime().availableProcessors());
    }

    /**
     * This convenience constructor defaults to no filename filtering and a max number of threads used equal to the
     * number of available processors, as returned by the runtime.
     *
     * @param client
     * @param earliest
     * @param latest
     * @param limit
     */
    public QueryLogIngestion(Client client, Calendar earliest, Calendar latest, int limit) {
        this(client, earliest, latest, limit, null, Runtime.getRuntime().availableProcessors());
    }

    /**
     * This convenience constructor defaults to the max number of threads used equal to the
     * number of available processors, as returned by the runtime.
     *
     * @param client
     * @param earliest
     * @param latest
     * @param limit
     * @param filenameFilter A compile regular expression. Any querylog whose name matches this pattern will be ignored.
     */
    public QueryLogIngestion(Client client, Calendar earliest, Calendar latest, int limit, Pattern filenameFilter) {
        this(client, earliest, latest, limit, filenameFilter, Runtime.getRuntime().availableProcessors());
    }

    public QueryLogIngestion(Client client) {
        this(client, null, null);
    }

    /**
     * search is a convenience function for initiating a graph search for all unfiltered log files.
     *
     * search uses a maximum number of threads equal to `numThread` given to the constructor. These threads are used
     * to download multiple QLI in parallel.
     *
     * @param root the root directory of the search.
     */
    public void search() {
        int prev = this.logs.size();
        for (Path root : this.client.roots()) {
            LOGGER.info("Searching through " + root.toString() + " ...");
            FileStatus r = new FileStatus();
            r.setPath(root);
            this.search(r);
            LOGGER.info("Found " + (this.logs.size() - prev) + " new logs in " + root.toString());
            prev = this.logs.size();
        }
        this.waitForCompletion();
        LOGGER.info(this.ioExceptions);
        LOGGER.info(this.remoteExceptions);
        try {
            new ObjectMapper().writeValue(this.out, this.logs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * children queries the remote HDFS REST API for a listing of the
     * provided directory. All nodes in the listing (whether they be a file or a directory)
     * are the "children" if this node.
     *
     * @param node
     * @return An iterator represting the "ls" of the given directory
     */
    @Override
    public Iterator<FileStatus> children(final FileStatus node) {
        this.state = State.values()[this.state.ordinal() + 1];
        Iterator<FileStatus> children = Arrays.asList(this.listStatus(node.getPath())).iterator();
        QueryLogIngestion self = this;
        return new Iterator<FileStatus>() {
            @Override
            public boolean hasNext() {
                boolean hasNext = children.hasNext();
                if (!hasNext) {
                    switch (self.state) {
                        case YEAR:
                            self.current.year = null;
                            break;
                        case MONTH:
                            self.current.month = null;
                            break;
                        case DAY:
                            self.current.day = null;
                            break;
                        case SECONDS:
                            break;
                    }
                    self.state = State.values()[self.state.ordinal() - 1];
                }
                return hasNext;
            }

            @Override
            public FileStatus next() {
                return children.next();
            }
        };
    }

    /**
     * A node is rejected from consideration in the graph search if and only if:
     *
     * 1. The log limit has been reached OR
     * 2. It is a file AND
     *  2.a. The file is not a log OR
     *  2.b  The file is outside of the date range OR
     *  2.c  The filename has been filtered out.
     *
     * @param node
     * @return
     */
    @Override
    public boolean reject(final FileStatus node) {
        if (node.isDirectory()) {
            int time;
            try {
                time = Integer.parseInt(node.getPath().getName());
            } catch (Throwable e) {
                LOGGER.warn("Found an unexpected directory, " + node.getPath().getName());
                return true;
            }
            switch (this.state) {
                case YEAR:
                    this.current.year = time;
                    break;
                case MONTH:
                    this.current.month = time;
                    break;
                case DAY:
                    this.current.day = time;
                    break;
                case SECONDS:
                    break;
                default:
                    throw new RuntimeException("asdas");
            }

            boolean rejected = !(this.earliest.before(this.current) && this.latest.after(this.current));
            LOGGER.debug("Directory " + this.current.constructDateString() + " was " + (rejected ? "rejected" : "accepted"));
            return rejected;
        }
        boolean rejected =  isNotALogFile(node) || isFilteredOut(node) || this.logLimitReached(node);
        LOGGER.debug("File " + node.getPath() + " was " + (rejected ? "rejected" : "accepted"));;
        return rejected;
    }

    private boolean logLimitReached(FileStatus file) {
        if (this.getQueryLog(extractJobID(file)) == null) {
            LOGGER.info("Log limit reached at " + file.getPath());
            return true;
        }
        return false;
    }

    /**
     * accept determines whether or not the node under considering is a logfile or not.
     *
     * Before returning, this function adds a job to the ThreadPool to download
     * and parse the given log file.
     *
     * @param leaf
     * @return
     */
    @Override
    public boolean accept(final FileStatus leaf) {
        if (leaf.isDirectory()) {
            return false;
        }
        LOGGER.debug("parsing " + leaf.getPath());
        // This is the one spot where we utilize the thread pool.
        // Parsing a file incurs its download, which can be expensive.
        this.execute(() -> this.parse(leaf));
        return true;
    }

    /**
     * parse downloads and parses the given query log.
     *
     * Any errors that may occur are logged in the ongoing exception vectors.
     *
     * @param log
     */
    public void parse(final FileStatus log) {
        InputStream is = this.open(log);
        if (is == null) {
            return;
        }
        switch (extractFileExtension(log)) {
            case HISTORY_FILE_SUFFIX:
                this.parseJHIST(is, log);
                break;
            case CONFIGURATION_FILE_SUFFIX:
                this.parseConfiguration(is, log);
                break;
        }
    }

    /**
     * parseJHIST parses the given input stream as an AVRO-JSON file that must be reflective of a org.apache.hadoop.mapreduce.jobhistory.
     * The result is used to populate the appropriate QueryLog record within {@link #logs}.
     *
     * Any errors that occur will be recorded in their respective exceptions vector
     * (see {@link #remoteExceptions}, {@link }, and {@link #ioExceptions}).
     *
     * @param is the input stream with which to parse.
     * @param log Used only for logging purposes.
     */
    public void parseJHIST(final InputStream is, final FileStatus log) {
        EventReader reader;
        HistoryEvent event;
        QueryLog ql;
        reader = this.makeEventReader(is, log);
        if (reader == null) {
            return;
        }
        ql = this.getQueryLog(extractJobID(log));
        if (ql == null) {
            // This should not occur as construction of this object must have occurred in this.reject
            // when asking if the log limit as been reached. However we need to protect ourselves from NPEs.
            this.ioExceptions.add(new IOException(
                    "Construction of a new QueryLog was attempted while parsing a .jhist file after the " +
                            "maximum limit of query log ingestion had been reached."));
            return;
        }
        boolean submissionEventFound = false;
        boolean startEventFound = false;
        boolean finishedEventFound = false;
        event = this.read(reader, log);
        while (event != null && (!submissionEventFound || !startEventFound || !finishedEventFound)) {
            // This type switch is non-exhaustive. The number of possible events is actually much greater.
            // see the following from org.apache.hadoop.mapreduce.jobhistory.EventReader for a comprehensive list of types.
            // https://github.com/apache/hadoop/blob/460a94a10f9c314b77a25e14efbf7c4dc3f5d9aa/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/jobhistory/EventReader.java#L98
            //
            // However, we are currently only interested in the information from when the job was submitted (who, and what?),
            // when it was started (when?), and when it finished (when? And how many bytes were written/read?).
            switch (event.getEventType()) {
                case JOB_SUBMITTED:
                    submissionEventFound = true;
                    JobSubmittedEvent submitted = (JobSubmittedEvent) event;
                    // The username in the job history and the username
                    // in the job configuration will always be the same, so just to
                    // make logic a bit simpler we are going to have both set the
                    // QueryStatement username, because it is apparently possible for
                    // one file to exist without the other.
                    ql.setUserName(submitted.getUserName());
                    // This is the actual query text.
                    ql.setText(submitted.getWorkflowName());
                    break;
                case AM_STARTED:
                    startEventFound = true;
                    // We are only interested in the start time.
                    AMStartedEvent started = (AMStartedEvent) event;
                    ql.setStartTime(started.getStartTime());
                    break;
                case JOB_FINISHED:
                    finishedEventFound = true;
                    // We are interested in the finish time and the statistics about the job.
                    // Specifically, we want to add up all of the bytes read/written to the filesystem and the the HDFS.
                    JobFinishedEvent finished = (JobFinishedEvent) event;
                    ql.setFinishTime(finished.getFinishTime());
                    Counters counters = finished.getTotalCounters();
                    long totalIoCount = 0;
                    totalIoCount += counters.findCounter(FILESYSTEM_COUNTER_GROUPNAME, "FILE_BYTES_READ").getValue();
                    totalIoCount += counters.findCounter(FILESYSTEM_COUNTER_GROUPNAME, "FILE_BYTES_WRITTEN").getValue();
                    totalIoCount += counters.findCounter(FILESYSTEM_COUNTER_GROUPNAME, "HDFS_BYTES_READ").getValue();
                    totalIoCount += counters.findCounter(FILESYSTEM_COUNTER_GROUPNAME, "HDFS_BYTES_WRITTEN").getValue();
                    ql.setTotalIoCount(totalIoCount);
                    break;
            }
            event = this.read(reader, log);
        }
    }

    /**
     * parseConfiguration parses the given input stream as an XML file that must be reflective of a org.apache.hadoop.mapred.JobConf.
     * The resulted is used to populate the appropriate QueryLog record within this.QLI.
     *
     * Any errors that occur will be recorded in their respective exceptions vector
     * (see {@link #remoteExceptions}, {@link }, and {@link #ioExceptions}).
     *
     * @param is the input stream with which to parse.
     * @param configuration Used only for logging purposes.
     */
    public void parseConfiguration(final InputStream is, final FileStatus configuration) {
        QueryLog ql = this.getQueryLog(extractJobID(configuration));
        if (ql == null) {
            // This should not occur as construction of this object must have occurred in this.reject
            // when asking if the log limit as been reached. However we need to protect ourselves from NPEs.
            this.ioExceptions.add(new IOException(
                    "Construction of a new QueryLog was attempted while parsing a .xml file after the " +
                            "maximum limit of query log ingestion had been reached."));
            return;
        }
        // Deserialize the XML to the native class provided in the mapred API.
        // The API allows for many arbitrary source of XML, but not via the constructor. The constructor accepts
        // a string, but that string is the PATH to an XML file and not the XML contents itself. As such, we have to
        // new up a JobConf and THEN add an input stream to read from.
        //
        // Otherwise actually a pretty nice API.
        final JobConf job = new JobConf();
        job.addResource(is);
        try {
            // Only the configuration has the session ID, the job history does not seem to.
                    //fs.adl.oauth2.client.id
            ql.setSessionId(job.get("hive.session.id") == null ? job.get("fs.adl.oauth2.client.id") : job.get("hive.session.id"));
            // This is the query string text. Equivalent to the JHIST workflowName.
            ql.setText(job.get("hive.query.string"));
            // The database queried.
            ql.setDefaultDatabases(job.get("hive.current.database"));
            // There are a number of places that the username can reside. We try two here
            // ("user.name" and the the name used by getUser which is "mapreduce.job.user.name"). The username also exists
            // in the job history, so if this XML file is missing then the JHIST parser will pick it up.
            ql.setUserName(Strings.isNullOrEmpty(job.getUser()) ? job.get("user.name") : job.getUser());
        } catch (RuntimeException e) {
            // Their API plays it fast and loose and calls incorrect file formats a runtime exceptions.
            // Yeah, well, we're more responsible than that, aren't we?
            try {
                is.reset();
                String fileContents = IOUtils.toString(is);
                this.ioExceptions.add(new IOException(
                        // Example output:
                        //
                        // The following error has occurred when trying to parse the job configuration file found on the remote HDFS at /mr-history/done/alice
                        // >>> Error: Event schema string not parsed since its null
                        // The following is the full contents of /mr-history/done/alice, is it actually a valid XML file?
                        // --------------------------------------------------------------------------------------------------
                        // bob
                        // --------------------------------------------------------------------------------------------------
                        "The following error has occurred when trying to parse the job configuration file found on the remote HDFS at " +
                                configuration.getPath() + "\n>>> Error: " + e.getMessage() + "\n" +
                                "The following is the full contents of " + configuration.getPath() + ", is it actually a valid XML file?\n" +
                                "--------------------------------------------------------------------------------------------------\n" +
                                fileContents + "\n" +
                                "--------------------------------------------------------------------------------------------------\n", e));
            } catch (IOException anotherE) {
                this.ioExceptions.add(new IOException(anotherE));
            }
        }
    }

    /**
     * extracJobID uses the filename of the provided FileStatus to construct a JobID.
     * See JOB_ID_REGEX for formatting expectations and result.
     *
     * @param file
     * @return
     */
    public static JobID extractJobID(final FileStatus file) {
        Matcher matcher = JOB_ID_REGEX.matcher(file.getPath().getName());
        if (!matcher.find()) {
            // Should not be reachable as the regex was matched just to get here. But for the sake of thoroughness...
            throw new RuntimeException("The provided file does not match a hive query log. Filename: " + file.getPath().getName());
        }
        return new JobID(matcher.group(JTIDENTIFIER), Integer.parseInt(matcher.group(ID)));
    }

    /**
     * extractFileExtensions uses the filename of the provided FileStatus to construct the file extension.
     * See JOB_ID_REGEX for formatting expectations and result.
     *
     * @param file
     * @return
     */
    public static String extractFileExtension(final FileStatus file) {
        Matcher matcher = JOB_ID_REGEX.matcher(file.getPath().getName());
        if (!matcher.find()) {
            // Should not be reachable as the regex was matched just to get here. But for the sake of thoroughness...
            throw new RuntimeException("The provided file does not match a hive query log. Filename: " + file.getPath().getName());
        }
        return matcher.group(FILE_EXTENSION);
    }

    /**
     * isNotALogFile determines whether the given file's name
     * matches with that which is generated by org.apache.hadoop.mapreduce.JobID
     *
     * @param file the file that we are checking
     * @return whether or not the provided file is a MapR logfile.
     */
    public static boolean isNotALogFile(final FileStatus file) {
        return !JOB_ID_REGEX.matcher(file.getPath().getName()).matches();
    }

    /**
     * isFilteredOut determines whether or not the provided file matches the user provided
     * regular expression for filtering.
     *
     * @param file the file that we are checking.
     * @return whether or not the provided file matches the user provided filter.
     */
    public boolean isFilteredOut(final FileStatus file) {
        return this.filenameFilter != null && this.filenameFilter.matcher(file.getPath().getName()).matches();
    }

    /**
     * getQueryLog returns the query statement under construction at the given job ID.
     * If the job ID does not yet have a query statement associated with it, then it is constructed
     * as a zero value before being returned.
     *
     * If a give JobID requires construction AND the query has already reached it maximum number of logs,
     * then null is returned.
     *
     * @param jobid
     * @return the query log under construction at the give jobid, or null if maximum query logs has been reached.
     */
    public synchronized QueryLog getQueryLog(final JobID jobid) {
        if (this.logTable.containsKey(jobid)) {
            return this.logTable.get(jobid);
        }
        if (this.logs.size() >= this.limit) {
            return null;
        }
        QueryLog ql = new QueryLog();
        this.logTable.put(jobid, ql);
        this.logs.add(ql);
        return ql;
    }

    /**
     * listStatus is a lifted function. It attempts to call "ls" on the remote directory.
     *
     * If an error occurs, then the error is recorded in the ongoing exception vectors and
     * an empty list is returned.
     *
     * @param path
     * @return
     */
    public FileStatus[] listStatus(final Path path) {
        try {
            return this.client.listStatus(path);
        } catch (IOException e) {
            this.ioExceptions.add(e);
        }
        return new FileStatus[]{};
    }

    /**
     * read is a lifted function to make exception handling a bit easier to read.
     *
     * A HistoryEvent reader will return null when it is done, and so does this function. This function will also return
     * null upon the receipt of an error, as an error indicates that no more reads can be done. The causing error
     * is recorded in the `ioExceptions` field.
     *
     * @param reader the to read events from
     * @param file the FileStatus of the file we are deserializing. This is given merely to provide for richer logging.
     * @return the next HistoryEvent within the reader, or null if either the reader is exhausted or an error is encountered.
     */
    public HistoryEvent read(final EventReader reader, final FileStatus file) {
        try {
            return reader.getNextEvent();
        } catch (IOException e) {
            this.ioExceptions.add(new IOException("The following log file appears to be malformed: " + file.getPath() + " " +  e.getMessage()));
            return null;
        }
    }

    /**
     * open is a lifted function. It makes a call out to the injected Hadoop client for an InputStream whose
     * contents are the contents of the request log file.
     *
     * Callers must check for null, as null may be returned if an error occurs.
     *
     * All errors are logged into {@link #remoteExceptions} and {}.
     *
     * @param log
     * @return The contents of the given log, or null if an error occurred.
     */
    public InputStream open(final FileStatus log) {
        try {
            return this.client.open(log.getPath());
        } catch (IOException e) {
            this.ioExceptions.add(e);
        }
        return null;
    }

    /**
     * makeReader is a lifted function. Any IOException that may occur during construction of the EventReader will
     * be logged in {@link #ioExceptions} and null will be returned.
     *
     * Callers must check for null returns.
     *
     * @param is An input stream for an AVRO-JSON jhist file.
     * @param log The filestatus associated with the InputStream. Used only for logging purposes.
     * @return the reader constructed from the provided InputStream, or null if construction failed.
     */
    public EventReader makeEventReader(final InputStream is, final FileStatus log) {
        try {
            return new EventReader(new DataInputStream(is));
        } catch (IOException e) {
            // The most likely cause of this exception is if the remote file is not properly formatted.
            // The most likely cause of said malformatting may be simple corruption of the file (read
            // the output of the file, it'll be apparent if it's a half baked JSON), or if the remote
            // schema has migrated (check the top of the history file - every Avro JSON file comes with
            // the schema at the top).
            //
            // If you suspect a schema migration by Hive, then follow the implementation of EventReader
            // to see what schemas that class is expecting versus what we got.
            try {
                is.reset();
                String fileContents = IOUtils.toString(is);
                this.ioExceptions.add(new IOException(
                        // Example output:
                        //
                        // The following error has occurred when trying to parse the job history file found on the remote HDFS at /mr-history/done/alice
                        // >>> Error: Event schema string not parsed since its null
                        // The following is the full contents of /mr-history/done/alice, is it actually a valid Avro-JSON file?
                        // --------------------------------------------------------------------------------------------------
                        // bob
                        // --------------------------------------------------------------------------------------------------
                        "The following error has occurred when trying to parse the job history file found on the remote HDFS at " +
                                log.getPath() + "\n>>> Error: " + e.getMessage() + "\n" +
                                "The following is the full contents of " + log.getPath() + ", is it actually a valid Avro-JSON file?\n" +
                                "--------------------------------------------------------------------------------------------------\n" +
                                fileContents + "\n" +
                                "--------------------------------------------------------------------------------------------------\n", e));
            } catch (IOException anotherE) {
                this.ioExceptions.add(new IOException(anotherE));
            }
            return null;
        }
    }

    /**
     * Execute executes the provided runnable within the thread pool.
     *
     * Functions executed are guaranteed to to add themselves to the waitGroup before
     * submission to the thread pool and are guaranteed to arrive at the waitGroup upon completion.
     *
     * Functions executed by this class should be executed using this method rather
     * than directly through the thread pool.
     *
     * @param function
     */
    private void execute(Runnable function) {
        this.waitGroup.register();
        this.pool.execute(() -> {
            try {
                function.run();
            } finally {
                this.waitGroup.arrive();
            }
        });
    }

    public void waitForCompletion() {
        this.waitGroup.register();
        this.waitGroup.arriveAndAwaitAdvance();
    }
}

