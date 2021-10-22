package dkarag.spark.utils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TimeUtils {
    public static Timestamp toStartOfTheDay(Timestamp timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar convertedTime = Calendar.getInstance();
        convertedTime.set(Calendar.MINUTE, 0);
        convertedTime.set(Calendar.SECOND, 0);
        convertedTime.set(Calendar.HOUR_OF_DAY, 0);
        return new Timestamp(convertedTime.getTimeInMillis());
    }
}
