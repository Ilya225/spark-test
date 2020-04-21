package spark.test.app.jobs;

import org.apache.spark.sql.*;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.test.app.AppConf;
import spark.test.app.TimeInterval;
import spark.test.app.utils.DateUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static scala.collection.JavaConverters.asScalaBufferConverter;
import static spark.test.app.AppConstants.*;

/**
 * Main job that uses Spark DataFrame API.
 */
public class SensorEventsSQLJob implements JobRunner {

    private final AppConf appConf;

    public SensorEventsSQLJob(AppConf appConf) {
        this.appConf = appConf;
    }

    private final static Logger logger = LoggerFactory.getLogger(SensorEventsSQLJob.class);

    public void run() {

        String start = appConf.getStartDate();
        String end = appConf.getEndDate();

        List<String> dateRange = DateUtils.getDateRange(LocalDateTime.parse(start), LocalDateTime.parse(end), TimeInterval.QUARTER_HOUR)
                .stream().map(date -> date.toString(PARTITION_DATE_PATTERN)).collect(Collectors.toList());

        try (SparkSession session = createSparkSession()) {

            logger.info("Creating timeslots with range: [from {} - to {}]", appConf.getStartDate(), appConf.getEndDate());
            Dataset<Row> dateRangeDF = broadcast(session.createDataset(dateRange, Encoders.STRING()).toDF(TIME_SLOT));

            DataFrameReader dataFrameReader = session.read();

            logger.info("Reading sensor metadata path [{}]", appConf.getMetadataPath());
            Dataset<Row> ids1 = dataFrameReader.option("header", "false").csv(appConf.getMetadataPath())
                    .toDF(SENSOR_ID, CHANEL_ID, CHANEL_TYPE, LOCATION_ID);

            logger.info("Reading events path: [{}]", appConf.getEventsPath());
            Dataset<Row> ids2 = session.read().option("header", "false").csv(appConf.getEventsPath())
                    .toDF(SENSOR_ID, CHANEL_ID, TIMESTAMP, VALUE, TIME_PARTITION)
                    .where(col(TIME_PARTITION).between(start, end));


            Dataset<Row> joinedDF = ids2.join(ids1, asScalaBufferConverter(Arrays.asList(SENSOR_ID, CHANEL_ID)).asScala(), "inner");

            Dataset<Row> result = getStatisticDataFrameWithTimeSlots(joinedDF);

            Dataset<Row> timeSlotEventsDF = joinWithTimeSlot(dateRangeDF, result);

            saveDataFrame(timeSlotEventsDF);
        }
    }

    private SparkSession createSparkSession() {
        logger.info("Creating spark session");
        return SparkSession.builder()
                .appName("SensorEventsSQLJob")
                .master(appConf.getMaster())
                .getOrCreate();
    }

    /**
     * Saves results in partitions for easier read.
     *
     * @param result
     */
    private void saveDataFrame(Dataset<Row> result) {
        result
                .withColumn(TIME_SLOT_START, col(TIME_SLOT))
                .withColumn(TIME_SLOT, col(TIME_SLOT).cast("date"))
                .write().mode(SaveMode.Overwrite)
                .partitionBy(TIME_SLOT)
                .json(appConf.getOutputPath());
    }

    /**
     * Not sure about this part, but since we need time slot entries even if there is no data, I just join it to
     * dataset with date values to fill all absent time slots. It's pretty simple and performant join, because all
     * logic is done before join, and it needs no shuffling.
     *
     * @param dateRangeDF
     * @param result
     * @return
     */
    public Dataset<Row> joinWithTimeSlot(Dataset<Row> dateRangeDF, Dataset<Row> result) {
        return dateRangeDF
                .join(result,
                        to_timestamp(col(TIME_SLOT)).equalTo(col("window").getField("start")), "leftouter");
    }

    /**
     * Method performs actual calculation and aggregation.
     *
     * @param timeSlotEventsDF
     * @return
     */
    public Dataset<Row> getStatisticDataFrameWithTimeSlots(Dataset<Row> timeSlotEventsDF) {
        return timeSlotEventsDF.select(
                col(VALUE).cast("double"),
                col(LOCATION_ID),
                to_timestamp(col(TIMESTAMP)).as("interval"),
                col(CHANEL_TYPE))
                .groupBy(col(LOCATION_ID), window(col("interval").as(TIME_SLOT), appConf.getInterval().getInterval()))
                .agg(
                        avg(when(col(CHANEL_TYPE).equalTo("temperature"), col(VALUE))).as(TEMP_AVG),
                        max(when(col(CHANEL_TYPE).equalTo("temperature"), col(VALUE))).as(TEMP_MAX),
                        min(when(col(CHANEL_TYPE).equalTo("temperature"), col(VALUE))).as(TEMP_MIN),
                        count(when(col(CHANEL_TYPE).equalTo("temperature"), col(VALUE))).as(TEMP_CNT),
                        count(when(col(CHANEL_TYPE)
                                .equalTo("presence").and(col(VALUE).equalTo("1")), col(VALUE))).as(PRESENCE_CNT))
                .withColumn(PRESENCE, when(col(PRESENCE_CNT).gt(0), "true").otherwise("false"));
    }

    @Override
    public AppConf getAppConf() {
        return appConf;
    }
}
