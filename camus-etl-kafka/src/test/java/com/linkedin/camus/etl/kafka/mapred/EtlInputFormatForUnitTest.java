package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.LeaderInfo;
import com.linkedin.camus.workallocater.CamusRequest;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import kafka.common.TopicAndPartition;
import kafka.javaapi.consumer.SimpleConsumer;


public class EtlInputFormatForUnitTest extends EtlInputFormat {

    public static SimpleConsumer consumer;
    public static ConsumerType consumerType = ConsumerType.REGULAR;
    public static RecordReaderClass recordReaderClass = RecordReaderClass.REGULAR;
    public static CamusRequestType camusRequestType = CamusRequestType.REGULAR;
    public EtlInputFormatForUnitTest() {
        super();
    }

    public static void modifyRequestOffsetTooEarly(CamusRequest etlRequest) {
        etlRequest.setEarliestOffset(-1L);
        etlRequest.setOffset(-2L);
        etlRequest.setLatestOffset(1L);
    }

    public static void modifyRequestOffsetTooLate(CamusRequest etlRequest) {
        etlRequest.setEarliestOffset(-1L);
        etlRequest.setOffset(2L);
        etlRequest.setLatestOffset(1L);
    }

    public static void reset() {
        consumerType = ConsumerType.REGULAR;
        recordReaderClass = RecordReaderClass.REGULAR;
        camusRequestType = CamusRequestType.REGULAR;
    }

    @Override
    public SimpleConsumer createSimpleConsumer(JobContext context, String host, int port) {
        switch (consumerType) {
            case REGULAR:
                return new SimpleConsumer(host, port, CamusJob.getKafkaTimeoutValue(context),
                                          CamusJob.getKafkaBufferSize(context),
                                          CamusJob.getKafkaClientName(context));
            case MOCK:
                return consumer;
            default:
                throw new RuntimeException("consumer type not found");
        }
    }

    @Override
    public RecordReader<EtlKey, CamusWrapper> createRecordReader(InputSplit split,
                                                                 TaskAttemptContext context)
            throws IOException, InterruptedException {
        switch (recordReaderClass) {
            case REGULAR:
                return new EtlRecordReader(this, split, context);
            case TEST:
                return new EtlRecordReaderForUnitTest(this, split, context);
            default:
                throw new RuntimeException("record reader class not found");
        }
    }

    @Override
    public ArrayList<CamusRequest> fetchLatestOffsetAndCreateEtlRequests(JobContext context,
                                                                         HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo) {
        ArrayList<CamusRequest> finalRequests = super
                .fetchLatestOffsetAndCreateEtlRequests(context, offsetRequestInfo);
        switch (camusRequestType) {
            case REGULAR:
                break;
            case MOCK_OFFSET_TOO_EARLY:
                for (CamusRequest request : finalRequests) {
                    modifyRequestOffsetTooEarly(request);
                }
                break;
            case MOCK_OFFSET_TOO_LATE:
                for (CamusRequest request : finalRequests) {
                    modifyRequestOffsetTooLate(request);
                }
                break;
            default:
                throw new RuntimeException("camus request class not found");
        }
        return finalRequests;
    }

    protected void writeRequests(List<CamusRequest> requests, JobContext context)
            throws IOException {
        //do nothing
    }

    public enum ConsumerType {
        REGULAR,
        MOCK
    }

    public enum RecordReaderClass {
        REGULAR,
        TEST
    }

    public enum CamusRequestType {
        REGULAR,
        MOCK_OFFSET_TOO_EARLY,
        MOCK_OFFSET_TOO_LATE
    }
}
