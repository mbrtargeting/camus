package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import com.linkedin.camus.etl.kafka.common.KafkaReader;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;


public class EtlRecordReader extends RecordReader<EtlKey, CamusWrapper> {

    private static final String PRINT_MAX_DECODER_EXCEPTIONS = "max.decoder.exceptions.to.print";
    private static final String DEFAULT_SERVER = "server";
    private static final String DEFAULT_SERVICE = "service";
    private static final Logger log = Logger.getLogger(EtlRecordReader.class);

    private final EtlKey key = new EtlKey();
    protected TaskAttemptContext context;
    private EtlSplit split;
    private final EtlInputFormat inputFormat;
    private Mapper<EtlKey, Writable, EtlKey, Writable>.Context mapperContext;
    private KafkaReader reader;
    private long totalBytes;
    private long readBytes = 0;
    private int numRecordsReadForCurrentPartition = 0;
    private long bytesReadForCurrentPartition = 0;
    private boolean skipSchemaErrors = false;
    private MessageDecoder decoder;
    private CamusWrapper value;
    private int maxPullHours = 0;
    private int exceptionCount = 0;
    private long maxPullTime = 0;
    private long endTimeStamp = 0;
    private long curTimeStamp = 0;
    private long startTime = 0;
    private HashSet<String> ignoreServerServiceList = null;
    private PeriodFormatter periodFormatter = null;
    private final StringBuffer statusMsg = new StringBuffer();

    /**
     * Record reader to fetch directly from Kafka
     */
    public EtlRecordReader(EtlInputFormat inputFormat, InputSplit split, TaskAttemptContext context)
            throws IOException,
                   InterruptedException {
        this.inputFormat = inputFormat;
        initialize(split, context);
    }

    public static int getMaximumDecoderExceptionsToPrint(JobContext job) {
        return job.getConfiguration().getInt(PRINT_MAX_DECODER_EXCEPTIONS, 10);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // For class path debugging
        log.info("classpath: " + System.getProperty("java.class.path"));
        ClassLoader loader = EtlRecordReader.class.getClassLoader();
        log.info("PWD: " + System.getProperty("user.dir"));
        log.info("classloader: " + loader.getClass());

        this.split = (EtlSplit) split;
        this.context = context;

        if (context instanceof Mapper.Context) {
            mapperContext = (Mapper.Context) context;
        }

        skipSchemaErrors = EtlInputFormat.getEtlIgnoreSchemaErrors(context);

        if (EtlInputFormat.getKafkaMaxPullHrs(context) != -1) {
            maxPullHours = EtlInputFormat.getKafkaMaxPullHrs(context);
        } else {
            endTimeStamp = Long.MAX_VALUE;
        }

        if (EtlInputFormat.getKafkaMaxPullMinutesPerTask(context) != -1) {
            startTime = System.currentTimeMillis();
            maxPullTime =
                    new DateTime(startTime)
                            .plusMinutes(EtlInputFormat.getKafkaMaxPullMinutesPerTask(context))
                            .getMillis();
        } else {
            maxPullTime = Long.MAX_VALUE;
        }

        ignoreServerServiceList = new HashSet<>();
        Collections.addAll(ignoreServerServiceList, EtlInputFormat
                .getEtlAuditIgnoreServiceTopicList(context));

        totalBytes = this.split.getLength();

        periodFormatter = new PeriodFormatterBuilder().appendMinutes().appendSuffix("m")
                .appendSeconds().appendSuffix("s").toFormatter();
    }

    @Override
    public synchronized void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @SuppressWarnings("unchecked")
    private CamusWrapper getWrappedRecord(Message message) throws IOException {
        CamusWrapper r = null;
        try {
            r = decoder.decode(message);
            mapperContext.getCounter(KAFKA_MSG.DECODE_SUCCESSFUL).increment(1);
        } catch (Exception e) {
            mapperContext.getCounter(KAFKA_MSG.SKIPPED_OTHER).increment(1);
            if (!skipSchemaErrors) {
                throw new IOException(e);
            }
        }
        return r;
    }

    @Override
    public float getProgress() throws IOException {
        if (getPos() == 0) {
            return 0f;
        }

        if (getPos() >= totalBytes) {
            return 1f;
        }
        return (float) ((double) getPos() / totalBytes);
    }

    private long getPos() {
        return readBytes;
    }

    @Override
    public EtlKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public CamusWrapper getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    private void logPartitionSummary() {
        String timeSpentOnPartition = periodFormatter.print(
                new Duration(startTime, System.currentTimeMillis()).toPeriod());
        log.info("Time spent on this partition = " + timeSpentOnPartition);
        log.info("Num of records read for this partition = "
                + numRecordsReadForCurrentPartition);
        log.info("Bytes read for this partition = " + bytesReadForCurrentPartition);
        log.info("Actual avg size for this partition = "
                + bytesReadForCurrentPartition
                / numRecordsReadForCurrentPartition);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (System.currentTimeMillis() > maxPullTime) {
            String maxMsg = "at " + new DateTime(curTimeStamp).toString();
            log.info("Kafka pull time limit reached");
            statusMsg.append(" max read ").append(maxMsg);
            context.setStatus(statusMsg.toString());
            log.info(key.getTopic() + " max read " + maxMsg);
            mapperContext.getCounter("total", "request-time(ms)").increment(reader.getFetchTime());
            closeReader();

            String topicNotFullyPulledMsg =
                    String.format(
                            "Topic %s:%d not fully pulled, max task time reached %s, pulled %d records",
                            key.getTopic(),
                            key.getPartition(), maxMsg, numRecordsReadForCurrentPartition);
            mapperContext.write(key, new ExceptionWritable(topicNotFullyPulledMsg));
            log.warn(topicNotFullyPulledMsg);

            logPartitionSummary();

            return false;
        }

        while (true) {
            try {

                if (reader == null || !reader.hasNext()) {
                    if (numRecordsReadForCurrentPartition != 0) {
                        logPartitionSummary();
                    }

                    EtlRequest request = (EtlRequest) split.popRequest();
                    if (request == null) {
                        return false;
                    }

                    // Reset start time, num of records read and bytes read
                    startTime = System.currentTimeMillis();
                    numRecordsReadForCurrentPartition = 0;
                    bytesReadForCurrentPartition = 0;

                    if (maxPullHours > 0) {
                        endTimeStamp = 0;
                    }

                    key.set(request.getTopic(), request.getLeaderId(), request.getPartition(),
                            request.getOffset(),
                            request.getOffset(), 0);
                    value = null;
                    log.info("\n\nProcessing request: " + request);
                    statusMsg.append(statusMsg.length() > 0 ? "; " : "");
                    statusMsg.append(request.getTopic()).append(":").append(request.getLeaderId())
                            .append(":").append(request.getPartition());
                    context.setStatus(statusMsg.toString());

                    if (reader != null) {
                        closeReader();
                    }
                    reader = new KafkaReader(inputFormat, context, request,
                                            CamusJob.getKafkaTimeoutValue(mapperContext),
                                            CamusJob.getKafkaBufferSize(mapperContext));

                    decoder = createDecoder(request.getTopic());
                }
                int count = 0;
                Message message;
                while ((message = reader.getNext(key)) != null) {
                    readBytes += key.getMessageSize();
                    count++;
                    numRecordsReadForCurrentPartition++;
                    bytesReadForCurrentPartition += key.getMessageSize();
                    context.progress();
                    mapperContext.getCounter("total", "data-read")
                            .increment(message.getPayload().length);
                    mapperContext.getCounter("total", "event-count").increment(1);

                    long tempTime = System.currentTimeMillis();
                    CamusWrapper wrapper;
                    try {
                        wrapper = getWrappedRecord(message);

                        if (wrapper == null) {
                            throw new RuntimeException("null record");
                        }
                    } catch (Exception e) {
                        if (exceptionCount < getMaximumDecoderExceptionsToPrint(context)) {
                            mapperContext.write(key, new ExceptionWritable(e));
                            log.info(e.getMessage());
                            exceptionCount++;
                        } else if (exceptionCount == getMaximumDecoderExceptionsToPrint(context)) {
                            log.info("The same exception has occurred for more than "
                                     + getMaximumDecoderExceptionsToPrint(context)
                                     + " records. All further exceptions will not be printed");
                        }
                        if (System.currentTimeMillis() > maxPullTime) {
                            exceptionCount = 0;
                            break;
                        }
                        continue;
                    }

                    curTimeStamp = wrapper.getTimestamp();
                    try {
                        key.setTime(curTimeStamp);
                        key.addAllPartitionMap(wrapper.getPartitionMap());
                        setServerService();
                    } catch (Exception e) {
                        mapperContext.write(key, new ExceptionWritable(e));
                        continue;
                    }

                    if (endTimeStamp == 0) {
                        DateTime time = new DateTime(curTimeStamp);
                        statusMsg.append(" begin read at ").append(time);
                        context.setStatus(statusMsg.toString());
                        log.info(key.getTopic() + " begin read at " + time.toString());
                        endTimeStamp = (time.plusHours(maxPullHours)).getMillis();
                    } else if (curTimeStamp > endTimeStamp) {
                        String maxMsg = "at " + new DateTime(curTimeStamp).toString();
                        log.info("Kafka Max history hours reached");
                        mapperContext.write(
                                key,
                                new ExceptionWritable(String.format(
                                        "Topic not fully pulled, max task time reached %s, pulled %d records",
                                        maxMsg,
                                        numRecordsReadForCurrentPartition)));
                        statusMsg.append(" max read ").append(maxMsg);
                        context.setStatus(statusMsg.toString());
                        log.info(key.getTopic() + " max read " + maxMsg);
                        mapperContext.getCounter("total", "request-time(ms)")
                                .increment(reader.getFetchTime());
                        closeReader();
                    }

                    long secondTime = System.currentTimeMillis();
                    value = wrapper;
                    long decodeTime = ((secondTime - tempTime));

                    mapperContext.getCounter("total", "decode-time(ms)").increment(decodeTime);

                    if (reader != null) {
                        mapperContext.getCounter("total", "request-time(ms)")
                                .increment(reader.getFetchTime());
                    }
                    return true;
                }
                log.info("Records read : " + count);
                reader = null;
            } catch (KafkaReader.MetadataFetchException e) {
                throw new IOException(e);
            } catch (Throwable t) {
                Exception e = new Exception(t.getLocalizedMessage(), t);
                e.setStackTrace(t.getStackTrace());
                mapperContext.write(key, new ExceptionWritable(e));
                reader = null;
            }
        }
    }

    protected MessageDecoder createDecoder(String topic) {
        return MessageDecoderFactory.createMessageDecoder(context, topic);
    }

    private void closeReader() throws IOException {
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                // not much to do here but skip the task
            } finally {
                reader = null;
            }
        }
    }

    private void setServerService() {
        if (ignoreServerServiceList.contains(key.getTopic()) || ignoreServerServiceList
                .contains("all")) {
            key.setServer(DEFAULT_SERVER);
            key.setService(DEFAULT_SERVICE);
        }
    }

    public enum KAFKA_MSG {
        DECODE_SUCCESSFUL,
        SKIPPED_SCHEMA_NOT_FOUND,
        SKIPPED_OTHER
    }
}
