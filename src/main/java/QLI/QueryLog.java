package QLI;


import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.TimeZone;

/**
 * QueryLog is a thread safe POJO used for Hive QLI.
 *
 * The end target for QLI is the {@link QueryStatement} which is not thread safe. Additionally,
 * this class will count the seconds taken to execute a query as the object is being built.
 */
public class QueryLog {

    /**
     * The session identifier, found within the query job's
     * configuration file under the key "hive.session.id"
     * Looks a bit like: 71f3f04b-f9a1-43eb-bc57-5bfb213d48f3
     */
    private volatile String sessionId = null;

    /**
     * The database used,found within the query job's
     * configuration file under the key "hive.current.database"
     */
    private volatile String defaultDatabases = null;

    /**
     * The user this query executed as is. This value can
     * be found either within the configuration under "user.name"
     * or "mapreduce.job.user.name". It can also be found in the history
     * file under JOB_SUBMITTED.userName (and, frankly, probably a lot of
     * other places as well).
     */
    private volatile  String userName = null;

    /**
     * The actual query text.
     */
    private volatile  String text = null;

    /**
     * When the query started. Formatted according to {@link #TIME_FORMAT}
     */
    private volatile long startTime = 0;

    /**
     * When the query finished. Formatted according to {@link #TIME_FORMAT}
     */
    private volatile long finishTime = 0;

    /**
     * The sum of all of the bytes read and written to
     * both the local filesystem and the Hadoop Filesystem.
     */
    private volatile long totalIoCount = 0;

    /**
     * secondsTaken computed to be the finish time minus the start time.
     * This member is computed while setting the start time and finish time.
     */
    private volatile double secondsTaken = 0;

    /**
     * The time format used for {@link #startTime} and {@link #finishTime}.
     */
    private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.S";

    /**
     * The timezone that we want our {@link #TIME_FORMATTER} to use.
     */
    private static final String TIME_ZONE = "UTC";

    /**
     * The time formatter used to format the {@link #startTime} and {@link #finishTime}.
     *
     * Uses the format at {@link #TIME_FORMAT} and the timezone at {@link #TIME_ZONE}
     */
    private static final SimpleDateFormat TIME_FORMATTER;



    static {
        TIME_FORMATTER = new SimpleDateFormat(TIME_FORMAT);
        TIME_FORMATTER.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
    }

    public synchronized String getSessionId() {
        return sessionId;
    }

    public synchronized void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public synchronized String getDefaultDatabases() {
        return defaultDatabases;
    }

    public synchronized void setDefaultDatabases(String defaultDatabases) {
        this.defaultDatabases = defaultDatabases;
    }

    public synchronized String getUserName() {
        return userName;
    }

    public synchronized void setUserName(String userName) {
        this.userName = userName;
    }

    public synchronized String getText() {
        return text;
    }

    public synchronized void setText(String text) {
        this.text = text;
    }

    public synchronized long getStartTime() {
        return startTime;
    }

    /**
     * setStartTime takes in a start time in milliseconds and converts it to
     * the desired time format. setStartTime also records the startTime within {@link #secondsTaken}.
     *
     * setStartTime may only be called once. Any further calls will result in a RuntimeException.
     */
    public synchronized void setStartTime(long startTime) {
        if (this.startTime != 0) {
            throw new RuntimeException("Incorrect attempt to set startTime twice.");
        }
        // Seriously, has to be a double. Longs are 64bit integers and floats are 32bit IEEE floats, which
        // means there is a A LOT of precision that can (and will) be lost.

        // Changing this to a float WILL result in a bug.
        this.secondsTaken -= ((double)startTime) / 1000;
        this.startTime = startTime;
    }

    public synchronized long getFinishTime() {
        return finishTime;
    }

    /**
     * setFinishTime takes in a finish time in milliseconds and converts it to
     * the desired time format. setFinishTime also records the finishTime within {@link #secondsTaken}.
     *
     * setFinishTime may only be called once. Any further calls will result in a RuntimeException.
     */
    public synchronized void setFinishTime(long finishTime) {
        if (this.finishTime != 0) {
            throw new RuntimeException("Incorrect attempt to set finishTime twice.");
        }
        // Seriously, has to be a double. Longs are 64bit integers and floats are 32bit IEEE floats, which
        // means there is a A LOT of precision that can (and will) be lost.
        //

        this.secondsTaken += ((double)finishTime) / 1000;
        this.finishTime = finishTime;
    }

    public synchronized long getTotalIoCount() {
        return totalIoCount;
    }

    public synchronized void setTotalIoCount(long totalIoCount) {
        this.totalIoCount = totalIoCount;
    }

    public synchronized double getSecondsTaken() {
        return secondsTaken;
    }

    @Override
    public String toString() {
        return "QueryLog{" +
                "sessionId='" + sessionId + '\'' +
                ", defaultDatabases='" + defaultDatabases + '\'' +
                ", userName='" + userName + '\'' +
                ", text='" + text + '\'' +
                ", startTime='" + startTime + '\'' +
                ", finishTime='" + finishTime + '\'' +
                ", totalIoCount=" + totalIoCount +
                ", secondsTaken=" + secondsTaken +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryLog queryLog = (QueryLog) o;
        return getStartTime() == queryLog.getStartTime() &&
                getFinishTime() == queryLog.getFinishTime() &&
                getTotalIoCount() == queryLog.getTotalIoCount() &&
                Double.compare(queryLog.getSecondsTaken(), getSecondsTaken()) == 0 &&
                Objects.equals(getSessionId(), queryLog.getSessionId()) &&
                Objects.equals(getDefaultDatabases(), queryLog.getDefaultDatabases()) &&
                Objects.equals(getUserName(), queryLog.getUserName()) &&
                Objects.equals(getText(), queryLog.getText());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSessionId(), getDefaultDatabases(), getUserName(), getText(), getStartTime(), getFinishTime(), getTotalIoCount(), getSecondsTaken());
    }
}

