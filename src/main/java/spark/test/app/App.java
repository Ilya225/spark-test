package spark.test.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import spark.test.app.jobs.JobRunner;
import spark.test.app.jobs.PartitionsInitJob;
import spark.test.app.jobs.RDDTransformJob;
import spark.test.app.jobs.SensorEventsSQLJob;

public class App {

    private final static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        AppConf appConf = CommandLine.populateCommand(new AppConf(), args);

        if (appConf.isUsageHelpRequested()) {
            CommandLine.usage(new AppConf(), System.out);
            System.exit(0);
        }

        JobRunner runner = new SensorEventsSQLJob(appConf);

        switch (appConf.getJobType()) {
            case SQL:
                break;
            case RDD:
                runner = new RDDTransformJob(appConf);
                break;
            case PARTITION:
                runner = new PartitionsInitJob(appConf);
                break;
            default:
                logger.error("No runner for job type: [{}]", appConf.getJobType());
                System.exit(1);
        }

        runner.run();

    }
}
