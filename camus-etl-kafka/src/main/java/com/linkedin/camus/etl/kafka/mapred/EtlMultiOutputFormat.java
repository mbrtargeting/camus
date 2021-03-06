package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.etl.Partitioner;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.StringRecordWriterProvider;
import com.linkedin.camus.etl.kafka.partitioner.DefaultPartitioner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * EtlMultiOutputFormat.
 *
 * File names are determined by output keys.
 */

public class EtlMultiOutputFormat extends FileOutputFormat<EtlKey, Object> {

    public static final String
            ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY
            = "etl.destination.path.topic.sub.dir";
    public static final String
            ETL_DESTINATION_PATH_TOPIC_SUBDIRFORMAT
            = "etl.destination.path.topic.sub.dirformat";
    public static final String
            ETL_DESTINATION_PATH_TOPIC_SUBDIRFORMAT_LOCALE
            = "etl.destination.path.topic.sub.dirformat.locale";
    public static final String ETL_RUN_MOVE_DATA = "etl.run.move.data";
    public static final String ETL_RUN_TRACKING_POST = "etl.run.tracking.post";
    public static final String ETL_DEFAULT_TIMEZONE = "etl.default.timezone";
    public static final String
            ETL_OUTPUT_FILE_TIME_PARTITION_MINS
            = "etl.output.file.time.partition.mins";
    public static final String KAFKA_MONITOR_TIME_GRANULARITY_MS = "kafka.monitor.time.granularity";
    public static final String ETL_DEFAULT_PARTITIONER_CLASS = "etl.partitioner.class";
    public static final String
            ETL_RECORD_WRITER_PROVIDER_CLASS
            = "etl.record.writer.provider.class";
    public static final String OFFSET_PREFIX = "offsets";
    public static final String ERRORS_PREFIX = "errors";
    public static final String REQUESTS_FILE = "requests.previous";
    private static final String ETL_STAGING_PATH = "etl.staging.path";
    private static EtlMultiOutputCommitter committer = null;
    private static Map<String, Partitioner>
            partitionersByTopic
            = new HashMap<>();

    private static final Logger log = Logger.getLogger(EtlMultiOutputFormat.class);

    @SuppressWarnings("unchecked")
    public static Class<RecordWriterProvider> getRecordWriterProviderClass(JobContext job) {
        return (Class<RecordWriterProvider>) job.getConfiguration()
                .getClass(ETL_RECORD_WRITER_PROVIDER_CLASS, StringRecordWriterProvider.class);
    }

    public static void setDestinationPath(JobContext job, Path dest) {
        job.getConfiguration().set(ETL_STAGING_PATH, dest.toString());
    }

    public static Path getDestinationPath(JobContext job) {
        return new Path(job.getConfiguration().get(ETL_STAGING_PATH));
    }

    public static Path getDestPathTopicSubDir(JobContext job) {
        return new Path(
                job.getConfiguration().get(ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY, "hourly"));
    }

    public static long getMonitorTimeGranularityMs(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_MONITOR_TIME_GRANULARITY_MS, 10) * 60000L;
    }

    public static int getEtlOutputFileTimePartitionMins(JobContext job) {
        return job.getConfiguration().getInt(ETL_OUTPUT_FILE_TIME_PARTITION_MINS, 60);
    }

    public static boolean isRunMoveData(JobContext job) {
        return job.getConfiguration().getBoolean(ETL_RUN_MOVE_DATA, true);
    }

    public static boolean isRunTrackingPost(JobContext job) {
        return job.getConfiguration().getBoolean(ETL_RUN_TRACKING_POST, false);
    }

    public static String getWorkingFileName(JobContext context, EtlKey key) throws IOException {
        Partitioner partitioner = getPartitioner(context, key.getTopic());
        return partitioner
                .getWorkingFileName(key.getTopic(), key.getLeaderId(), key.getPartition(),
                                    partitioner.encodePartition(context, key));
    }

    public static Partitioner getDefaultPartitioner(JobContext job) {
        return ReflectionUtils.newInstance(
                job.getConfiguration()
                        .getClass(ETL_DEFAULT_PARTITIONER_CLASS, DefaultPartitioner.class,
                                  Partitioner.class),
                job.getConfiguration());
    }

    public static Partitioner getPartitioner(JobContext job, String topicName) {
        String customPartitionerProperty = ETL_DEFAULT_PARTITIONER_CLASS + "." + topicName;
        if (partitionersByTopic.get(customPartitionerProperty) != null) {
            return partitionersByTopic.get(customPartitionerProperty);
        }
        final Partitioner defaultPartitioner = getDefaultPartitioner(job);
        partitionersByTopic.put(customPartitionerProperty, defaultPartitioner);
        return defaultPartitioner;
    }

    @Override
    public RecordWriter<EtlKey, Object> getRecordWriter(TaskAttemptContext context)
            throws IOException,
                   InterruptedException {
        if (committer == null) {
            committer = new EtlMultiOutputCommitter(getOutputPath(context), context, log);
        }
        return new EtlMultiOutputRecordWriter(context, committer);
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException {
        if (committer == null) {
            committer = new EtlMultiOutputCommitter(getOutputPath(context), context, log);
        }
        return committer;
    }
}
