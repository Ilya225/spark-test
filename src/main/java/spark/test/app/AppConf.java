package spark.test.app;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.Serializable;

import static spark.test.app.AppConstants.*;

@Command
public final class AppConf implements Serializable {

    @Option(names = {"--type"}, description = "Job type to run: ${COMPLETION-CANDIDATES}", defaultValue = "SQL", required = true)
    private JobType jobType;

    @Option(names = {"--start"}, description = "Sensor Events Start Date", defaultValue = DEF_START)
    private String startDate;

    @Option(names = {"--end"}, description = "Sensor Events End Date", defaultValue = DEF_END)
    private String endDate;

    @Option(names = {"--master"}, description = "Spark master url", defaultValue = DEF_MASTER)
    private String master;

    @Option(names = {"--metadata"}, description = "Path to sensors metadata dataset", defaultValue = DEF_META_PATH)
    private String metadataPath;

    @Option(names = {"--events"}, description = "Path to sensor events dataset", defaultValue = DEF_VALUES_PATH)
    private String eventsPath;

    @Option(names = {"--output"}, description = "Job output path", defaultValue = DEF_JOB_OUTPUT)
    private String outputPath;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Shows help")
    private boolean usageHelpRequested;

    @Option(names = {"--interval"}, description = "Interval for sql job: ${COMPLETION-CANDIDATES}", defaultValue = "QUARTER_HOUR")
    private TimeInterval interval;

    /**
     * Prints usage.
     *
     * @param args
     */
    public static void main(String... args) {
        CommandLine.usage(new AppConf(), System.out);
    }

    public String getStartDate() {
        return startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public String getMaster() {
        return master;
    }

    public String getMetadataPath() {
        return metadataPath;
    }

    public TimeInterval getInterval() {
        return interval;
    }

    public String getEventsPath() {
        return eventsPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public JobType getJobType() {
        return jobType;
    }

    public boolean isUsageHelpRequested() {
        return usageHelpRequested;
    }
}
