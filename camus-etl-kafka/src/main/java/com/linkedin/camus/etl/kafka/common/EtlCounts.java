package com.linkedin.camus.etl.kafka.common;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


@JsonIgnoreProperties({"trackingCount", "lastKey", "eventCount", "RANDOM"})
public class EtlCounts {

    private static final String TOPIC = "topic";
    private static final String GRANULARITY = "granularity";
    private static final String COUNTS = "counts";
    private static final String START_TIME = "startTime";
    private static final String END_TIME = "endTime";
    private static final String FIRST_TIMESTAMP = "firstTimestamp";
    private static final String LAST_TIMESTAMP = "lastTimestamp";
    private static final String ERROR_COUNT = "errorCount";
    protected HashMap<String, Source> counts;
    private String topic;
    private long startTime;
    private long granularity;
    private long errorCount;
    private long endTime;
    private long lastTimestamp;
    private long firstTimestamp = Long.MAX_VALUE;
    private transient EtlKey lastKey;
    private transient int eventCount = 0;

    public EtlCounts(String topic, long granularity, long currentTime) {
        this.topic = topic;
        this.granularity = granularity;
        this.startTime = currentTime;
        this.counts = new HashMap<>();
    }

    public EtlCounts(String topic, long granularity) {
        this(topic, granularity, System.currentTimeMillis());
    }

    public HashMap<String, Source> getCounts() {
        return counts;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(long errorCount) {
        this.errorCount = errorCount;
    }

    public long getFirstTimestamp() {
        return firstTimestamp;
    }

    public void setFirstTimestamp(long firstTimestamp) {
        this.firstTimestamp = firstTimestamp;
    }

    public long getGranularity() {
        return granularity;
    }

    public void setGranularity(long granularity) {
        this.granularity = granularity;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getEventCount() {
        return eventCount;
    }

    public EtlKey getLastKey() {
        return lastKey;
    }

    public void incrementMonitorCount(EtlKey key) {
        long monitorPartition = DateUtils.getPartition(granularity, key.getTime());
        Source source = new Source(key.getServer(), key.getService(), monitorPartition);
        if (counts.containsKey(source.toString())) {
            Source countSource = counts.get(source.toString());
            countSource.setCount(countSource.getCount() + 1);
            counts.put(countSource.toString(), countSource);
        } else {
            source.setCount(1);
            counts.put(source.toString(), source);
        }

        if (key.getTime() > lastTimestamp) {
            lastTimestamp = key.getTime();
        }

        if (key.getTime() < firstTimestamp) {
            firstTimestamp = key.getTime();
        }

        lastKey = new EtlKey(key);
        eventCount++;
    }

    public void writeCountsToMap(ArrayList<Map<String, Object>> allCountObject) {
        Map<String, Object> countFile = new HashMap<>();
        countFile.put(TOPIC, topic);
        countFile.put(GRANULARITY, granularity);
        countFile.put(COUNTS, counts);
        countFile.put(START_TIME, startTime);
        countFile.put(END_TIME, endTime);
        countFile.put(FIRST_TIMESTAMP, firstTimestamp);
        countFile.put(LAST_TIMESTAMP, lastTimestamp);
        countFile.put(ERROR_COUNT, errorCount);
        allCountObject.add(countFile);
    }

}
