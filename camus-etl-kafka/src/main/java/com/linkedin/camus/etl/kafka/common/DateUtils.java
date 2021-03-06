package com.linkedin.camus.etl.kafka.common;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class DateUtils {

    public static final DateTimeZone PST = DateTimeZone.forID("America/Los_Angeles");

    public static DateTimeFormatter getDateTimeFormatter(String str) {
        return getDateTimeFormatter(str, PST);
    }

    public static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
        return DateTimeFormat.forPattern(str).withZone(timeZone);
    }

    public static long getPartition(long timeGranularityMs, long timestamp) {
        return (timestamp / timeGranularityMs) * timeGranularityMs;
    }

    public static long getPartition(long timeGranularityMs, long timestamp,
                                    DateTimeZone outputDateTimeZone) {
        long adjustedTimeStamp = outputDateTimeZone.convertUTCToLocal(timestamp);
        long partitionedTime = getPartition(timeGranularityMs, adjustedTimeStamp);
        return outputDateTimeZone.convertLocalToUTC(partitionedTime, false);
    }

}
