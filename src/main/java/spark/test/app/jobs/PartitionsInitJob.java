package spark.test.app.jobs;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.test.app.AppConf;

import static org.apache.spark.sql.functions.*;
import static spark.test.app.AppConstants.*;

/**
 * Since we need to read events by date values. I decided to partition the date by timestamp. Works only locally, but
 * it's pretty common practice to use data partitions to query the data in Hive like manner.
 */
public class PartitionsInitJob implements JobRunner {

    private final AppConf appConf;

    public PartitionsInitJob(AppConf appConf) {
        this.appConf = appConf;
    }

    @Override
    public void run() {
        try (SparkSession session = SparkSession.builder()
                .appName("PartitionInitJob")
                .master(appConf.getMaster())
                .getOrCreate()) {

            DataFrameReader dataFrameReader = session.read();

            Dataset<Row> ids2 = dataFrameReader.option("header", "false").csv(appConf.getEventsPath())
                    .toDF(SENSOR_ID, CHANEL_ID, TIMESTAMP, VALUE);

            ids2.withColumn(TIME_PARTITION, to_date(col(TIMESTAMP)))
                    .write()
                    .partitionBy(TIME_PARTITION)
                    .csv(appConf.getOutputPath());
        }
    }

    @Override
    public AppConf getAppConf() {
        return appConf;
    }
}
