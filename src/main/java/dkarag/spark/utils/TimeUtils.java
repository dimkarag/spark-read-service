package dkarag.spark.utils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TimeUtils {
    public static Timestamp toStartOfTheDay(Timestamp timestamp) {
        Calendar convertedTime = Calendar.getInstance();
        convertedTime.setTime(timestamp);
        convertedTime.set(Calendar.MINUTE, 0);
        convertedTime.set(Calendar.SECOND, 0);
        convertedTime.set(Calendar.HOUR_OF_DAY, 0);
        return new Timestamp(convertedTime.getTimeInMillis());
    }
    public static Timestamp toEndOfTheDay(Timestamp timestamp) {
        Calendar convertedTime = Calendar.getInstance();
        convertedTime.setTime(timestamp);
        convertedTime.set(Calendar.MINUTE, 59);
        convertedTime.set(Calendar.SECOND, 59);
        convertedTime.set(Calendar.HOUR_OF_DAY, 23);
        return new Timestamp(convertedTime.getTimeInMillis());
    }

    public static String format(Timestamp timestamp, String pattern) {
        SimpleDateFormat formatter = new SimpleDateFormat(pattern);
        return formatter.format(timestamp);
    }

    public static Timestamp toTimestamp(String date) throws ParseException {
        try {
            return Timestamp.valueOf(date);
        } catch (Exception e) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            return new Timestamp(formatter.parse(date).getTime());
        }
    }
}
