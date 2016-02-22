package com.linkedin.camus.etl.kafka.mapred;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.annotation.ParametersAreNonnullByDefault;


public class EtlMultiOutputRecordWriter extends RecordWriter<EtlKey, Object> {

    public static final String ETL_MAX_OPEN_WRITERS = "etl.writers.open.max";
    private static final Logger log = Logger.getLogger(EtlMultiOutputRecordWriter.class);
    private final Counter topicSkipOldCounter;
    /**
     * We need a Cache here to be able to limit the amount of open RecordWriters. Too many open
     * writers lead to memory
     * exceptions. Please ensure that your data does not need more open files than have writers.
     * But
     * this should not be
     * the case if you have data with a constantly growing timestamp.
     */
    private final Cache<String, RecordWriter<IEtlKey, CamusWrapper>> dataWriters;
    private final TaskAttemptContext context;
    private Writer errorWriter = null;
    private String currentTopic = "";
    private long beginTimeStamp = 0;
    private final EtlMultiOutputCommitter committer;

    public EtlMultiOutputRecordWriter(TaskAttemptContext context, EtlMultiOutputCommitter committer)
            throws IOException, InterruptedException {
        this.context = context;
        this.committer = committer;
        final String uniqueFile =
                EtlMultiOutputFormat.getUniqueFile(context, EtlMultiOutputFormat.ERRORS_PREFIX, "");
        errorWriter =
                SequenceFile.createWriter(FileSystem.get(context.getConfiguration()),
                                          context.getConfiguration(),
                                          new Path(committer.getWorkPath(), uniqueFile),
                                          EtlKey.class,
                                          ExceptionWritable.class);

        if (EtlInputFormat.getKafkaMaxHistoricalDays(context) != -1) {
            int maxDays = EtlInputFormat.getKafkaMaxHistoricalDays(context);
            beginTimeStamp = (new DateTime()).minusDays(maxDays).getMillis();
        } else {
            beginTimeStamp = 0;
        }
        log.info("beginTimeStamp set to: " + beginTimeStamp);
        topicSkipOldCounter = getTopicSkipOldCounter();

        final int maxOpenWriters = context.getConfiguration().getInt(ETL_MAX_OPEN_WRITERS, 100);
        log.info(String.format("Limiting open writers to %d", maxOpenWriters));

        dataWriters = CacheBuilder.newBuilder().maximumSize(maxOpenWriters)
                .removalListener(new WriterRemovalListener(context)).build();
    }

    private Counter getTopicSkipOldCounter() {
        try {
            //In Hadoop 2, TaskAttemptContext.getCounter() is available
            Method getCounterMethod = context.getClass()
                    .getMethod("getCounter", String.class, String.class);
            return ((Counter) getCounterMethod.invoke(context, "total", "skip-old"));
        } catch (NoSuchMethodException e) {
            //In Hadoop 1, TaskAttemptContext.getCounter() is not available
            //Have to cast context to TaskAttemptContext in the mapred package, then get a StatusReporter instance
            org.apache.hadoop.mapred.TaskAttemptContext
                    mapredContext
                    = (org.apache.hadoop.mapred.TaskAttemptContext) context;
            return ((StatusReporter) mapredContext.getProgressible())
                    .getCounter("total", "skip-old");
        } catch (IllegalArgumentException e) {
            log.error("IllegalArgumentException while obtaining counter 'total:skip-old': " + e
                    .getMessage());
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            log.error("IllegalAccessException while obtaining counter 'total:skip-old': " + e
                    .getMessage());
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            log.error("InvocationTargetException obtaining counter 'total:skip-old': " + e
                    .getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        dataWriters.invalidateAll();
        errorWriter.close();
    }

    @Override
    public void write(EtlKey key, Object val) throws IOException, InterruptedException {
        if (val instanceof CamusWrapper<?>) {
            if (key.getTime() < beginTimeStamp) {
                //TODO: fix this logging message, should be logged once as a total count of old records skipped for each topic
                // for now, commenting this out
                //log.warn("Key's time: " + key + " is less than beginTime: " + beginTimeStamp);
                topicSkipOldCounter.increment(1);
                committer.addOffset(key);
            } else {
                if (!key.getTopic().equals(currentTopic)) {
                    dataWriters.invalidateAll();
                    currentTopic = key.getTopic();
                }

                committer.addCounts(key);
                CamusWrapper value = (CamusWrapper) val;
                String workingFileName = EtlMultiOutputFormat.getWorkingFileName(context, key);

                RecordWriter<IEtlKey, CamusWrapper> recordWriter = dataWriters
                        .getIfPresent(workingFileName);
                if (recordWriter == null) {
                    recordWriter = getDataRecordWriter(context, workingFileName, value);
                    dataWriters.put(workingFileName, recordWriter);

                    log.info("Writing to data file: " + workingFileName);
                }
                recordWriter.write(key, value);
            }
        } else if (val instanceof ExceptionWritable) {
            committer.addOffset(key);
            log.warn("ExceptionWritable key: " + key + " value: " + val);
            errorWriter.append(key, (ExceptionWritable) val);
        } else {
            log.warn("Unknow type of record: " + val);
        }
    }

    private RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context,
                                                                    String fileName,
                                                                    CamusWrapper value) {
        try {
            Class<RecordWriterProvider> rwp = EtlMultiOutputFormat
                    .getRecordWriterProviderClass(context);
            Constructor<RecordWriterProvider> crwp = rwp.getConstructor(TaskAttemptContext.class);
            RecordWriterProvider recordWriterProvider = crwp.newInstance(context);
            return recordWriterProvider.getDataRecordWriter(context, fileName, value, committer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class WriterRemovalListener
            implements RemovalListener<String, RecordWriter<IEtlKey, CamusWrapper>> {

        private final TaskAttemptContext context;

        public WriterRemovalListener(final TaskAttemptContext context) {
            this.context = context;
        }

        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(
                final RemovalNotification<String, RecordWriter<IEtlKey, CamusWrapper>> removalNotification) {
            try {
                final RecordWriter<IEtlKey, CamusWrapper> recordWriter = removalNotification
                        .getValue();
                if (recordWriter != null) {
                    log.info("Invalidating writer for " + removalNotification.getKey());
                    removalNotification.getValue().close(context);
                }
            } catch (final IOException | InterruptedException e) {
                log.error(e);
            }
        }
    }
}
