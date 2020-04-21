package spark.test.app.jobs;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.Days;
import org.joda.time.LocalDateTime;
import scala.Tuple2;
import spark.test.app.AppConf;
import spark.test.app.TimeInterval;
import spark.test.app.dto.PeriodSensorData;
import spark.test.app.utils.DateUtils;

import java.util.List;
import java.util.stream.Collectors;

import static spark.test.app.AppConstants.*;

/**
 * Task2 Writes 1 hour interval dataset using 15 minutes interval dataset.
 */
public class RDDTransformJob implements JobRunner {

    private final static Gson gson = new Gson();
    private final AppConf appConf;


    public RDDTransformJob(AppConf appConf) {
        this.appConf = appConf;
    }

    @Override
    public void run() {
        String path = getSourcePath(appConf.getStartDate(), appConf.getEndDate());

        SparkConf conf = new SparkConf().setAppName("RDDTransform")
                .set("spark.hadoop.validateOutputSpecs", "false")
                .setMaster(appConf.getMaster());

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> quarterHourDataset = sc.textFile(path);

            JavaRDD<String> result = quarterHourDataset.map(row -> gson.fromJson(row, PeriodSensorData.class))
                    // here I just added a counter for temperature to be able to count average temperature in next step.
                    .map(period -> new Tuple2<>(period, period.getTempCnt() > 0 ? 1 : 0))
                    .keyBy(this::generateKey)
                    .reduceByKey((period1, period2) -> new Tuple2<>(mergePeriods(period1._1, period2._1), period1._2 + period2._2))
                    // calculation of average.
                     .mapValues(periodTuple -> {
                         if (periodTuple._1.getTempAvg() > 0) {
                             periodTuple._1.setTempAvg(periodTuple._1.getTempAvg() / periodTuple._2);
                         }
                         return periodTuple._1;
                     })
                    .sortByKey(true, Days.daysBetween(LocalDateTime.parse(appConf.getStartDate()),
                            LocalDateTime.parse(appConf.getEndDate())).getDays())
                    .map(period -> gson.toJson(period._2));

            result.saveAsTextFile(appConf.getOutputPath());

        }
    }

    @Override
    public AppConf getAppConf() {
        return appConf;
    }

    /**
     * Key represented as Location:TimeSlot, so we are able to group by timeslots and locations.
     * @param periodTuple
     * @return
     */
    public String generateKey(Tuple2<PeriodSensorData, Integer> periodTuple) {
        return periodTuple._1.getLocation() + ":" + LocalDateTime.parse(periodTuple._1.getTimeSlotStart(), PARTITION_DATE_FMT)
                .toString(PARTITION_BY_HOUR_PATTERN);
    }

    private String getSourcePath(String startDate, String endDate) {
        if (!startDate.isEmpty() && !endDate.isEmpty()) {
            List<String> dateRange = DateUtils
                    .getDateRange(LocalDateTime.parse(startDate), LocalDateTime.parse(endDate), TimeInterval.DAY)
                    .stream()
                    .map(date -> appConf.getEventsPath() + "TimeSlot=" + date.toString(PARTITION_PATH_PATTERN))
                    .collect(Collectors.toList());
            return String.join(",", dateRange);
        } else {
            return appConf.getEventsPath() + "*/*";
        }
    }

    private PeriodSensorData mergePeriods(PeriodSensorData period1, PeriodSensorData period2) {
        PeriodSensorData mergedPeriod = new PeriodSensorData();
        mergedPeriod.setPresence(period1.isPresence() || period2.isPresence());
        mergedPeriod.setPresenceCnt(period1.getPresenceCnt() + period2.getPresenceCnt());
        mergedPeriod.setTempCnt(period1.getTempCnt() + period2.getTempCnt());
        mergedPeriod.setTimeSlotStart(LocalDateTime.parse(period1.getTimeSlotStart(), PARTITION_DATE_FMT).toString(PARTITION_BY_HOUR_PATTERN));

        // Here average contains sum of averages, actual average counts in next stage.
        mergedPeriod.setTempAvg(period1.getTempAvg() + period2.getTempAvg());

        mergedPeriod.setTempMax(Math.max(period1.getTempMax(), period2.getTempMax()));

        // checks for at least one measurement are present in period, because if
        // there is no such, the min value would be default 0 which is not correct. We have to be sure that
        // we set some measured value. If both are present than we set minimum if only one presents than
        // we set bigger value.
        if (period1.getTempCnt() != 0 && period2.getTempCnt() != 0) {
            mergedPeriod.setTempMin(Math.min(period1.getTempMax(), period2.getTempMax()));
        } else if (period1.getTempCnt() != 0 || period2.getTempCnt() != 0) {
            mergedPeriod.setTempMin(Math.max(period1.getTempMax(), period2.getTempMax()));
        }

        return mergedPeriod;
    }
}
