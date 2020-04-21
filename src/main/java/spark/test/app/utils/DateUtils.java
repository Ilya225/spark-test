package spark.test.app.utils;

import org.joda.time.LocalDateTime;
import org.joda.time.Minutes;
import spark.test.app.TimeInterval;

import java.util.ArrayList;
import java.util.List;

public final class DateUtils {

    private DateUtils() {}

    public static List<LocalDateTime> getDateRange(LocalDateTime start, LocalDateTime end, TimeInterval interval) {

        int minutes = Minutes.minutesBetween(start, end).getMinutes();
        List<LocalDateTime> ret = new ArrayList<>();

        for (int i = 0; i < minutes; i += interval.getIntervalInMinutes()) {
            ret.add(start.plusMinutes(i));
        }

        return ret;
    }
}
