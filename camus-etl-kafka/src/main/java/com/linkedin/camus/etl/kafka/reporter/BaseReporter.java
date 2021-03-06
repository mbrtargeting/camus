package com.linkedin.camus.etl.kafka.reporter;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Map;


public abstract class BaseReporter {

    public static org.apache.log4j.Logger log;

    public BaseReporter() {
        log = org.apache.log4j.Logger.getLogger(BaseReporter.class);
    }

    public abstract void report(Job job, Map<String, Long> timingMap) throws IOException;
}
