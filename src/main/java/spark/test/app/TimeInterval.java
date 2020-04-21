package spark.test.app;

public enum TimeInterval {
    HOUR(60, "60 minutes"),
    QUARTER_HOUR(15, "15 minutes"),
    DAY(60*24, "1 day");

    private final int intervalInMinutes;
    private final String interval;

    TimeInterval(int intervalInMinutes, String interval) {
        this.intervalInMinutes = intervalInMinutes;
        this.interval = interval;
    }

    public int getIntervalInMinutes() {
        return intervalInMinutes;
    }

    public String getInterval() {
        return interval;
    }
}
