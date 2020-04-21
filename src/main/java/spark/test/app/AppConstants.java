package spark.test.app;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public final class AppConstants {

    private AppConstants() {}

    // Application arguments
    public final static String DEF_MASTER = "local[*]";
    public final static String DEF_START = "2018-03-23";
    public final static String DEF_END = "2018-03-26";
    public final static String DEF_META_PATH = "src/main/resources/ds1.csv";
    public final static String DEF_VALUES_PATH = "src/main/resources/ds2_part";
    public final static String DEF_JOB_OUTPUT = "src/main/resources/json/";



    // Date related constants
    public final static String PARTITION_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public final static String PARTITION_PATH_PATTERN = "yyyy-MM-dd";
    public final static String PARTITION_BY_HOUR_PATTERN = "yyyy-MM-dd HH:00:00";
    public final static DateTimeFormatter PARTITION_DATE_FMT = DateTimeFormat.forPattern(PARTITION_DATE_PATTERN);


    // RDD Application configuration
    public final static String RDD_OUTPUT_PATH = "src/main/resources/rdd_json/";

    // Sensor Event fields names
    public final static String SENSOR_ID = "SensorId";
    public final static String CHANEL_ID = "ChanelId";
    public final static String CHANEL_TYPE = "ChanelType";
    public final static String LOCATION_ID = "LocationId";
    public final static String TIMESTAMP = "Timestamp";
    public final static String VALUE = "Value";
    public final static String TIME_PARTITION = "Time_partition";
    public final static String TIME_SLOT = "TimeSlot";
    public final static String TIME_SLOT_START = "TimeSlotStart";
    public final static String PRESENCE_CNT = "PresenceCnt";
    public final static String PRESENCE = "Presence";
    public final static String TEMP_AVG = "TempAvg";
    public final static String TEMP_MIN = "TempMin";
    public final static String TEMP_MAX = "TempMax";
    public final static String TEMP_CNT = "TempCnt";

}
