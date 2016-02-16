package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import kafka.api.PartitionFetchInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;


/**
 * Poorly named class that handles kafka pull events within each
 * KafkaRecordReader.
 *
 * @author Richard Park
 */
public class KafkaReader {

    // index of context
    private static Logger log = Logger.getLogger(KafkaReader.class);
    private EtlRequest kafkaRequest = null;
    private SimpleConsumer simpleConsumer = null;

    private long beginOffset;
    private long currentOffset;
    private long lastOffset;
    private long currentCount;

    private TaskAttemptContext context;

    private Iterator<MessageAndOffset> messageIter = null;

    private long totalFetchTime = 0;
    private long lastFetchTime = 0;

    private int fetchBufferSize;

    /**
     * Construct using the json representation of the kafka request
     */
    public KafkaReader(EtlInputFormat inputFormat, TaskAttemptContext context, EtlRequest request,
                       int clientTimeout,
                       int fetchBufferSize) throws Exception, MetadataFetchException {
        this.fetchBufferSize = fetchBufferSize;
        this.context = context;

        log.info("bufferSize=" + fetchBufferSize);
        log.info("timeout=" + clientTimeout);

        // Create the kafka request from the json

        kafkaRequest = request;

        beginOffset = request.getOffset();
        currentOffset = request.getOffset();
        lastOffset = request.getLastOffset();
        currentCount = 0;
        totalFetchTime = 0;

        // read data from queue

        URI uri = kafkaRequest.getURI();
        simpleConsumer = inputFormat.createSimpleConsumer(context, uri.getHost(), uri.getPort());
        log.info("Connected to leader " + uri + " beginning reading at offset " + beginOffset
                 + " latest offset="
                 + lastOffset);
    }

    public boolean hasNext() throws IOException, MetadataFetchException {
        return (currentOffset < lastOffset)
               && (messageIter != null && messageIter.hasNext() || fetch());
    }

    /**
     * Fetches the next Kafka message and stuffs the results into the key and
     * value
     *
     * @return true if there exists more events
     */
    public KafkaMessage getNext(EtlKey etlKey) throws IOException, MetadataFetchException {
        if (hasNext()) {

            MessageAndOffset msgAndOffset = messageIter.next();
            Message message = msgAndOffset.message();

            byte[] payload = getBytes(message.payload());
            byte[] key = getBytes(message.key());

            if (payload == null) {
                log.warn("Received message with null message.payload(): " + msgAndOffset);
            }

            etlKey.clear();
            etlKey.set(kafkaRequest.getTopic(), kafkaRequest.getLeaderId(),
                       kafkaRequest.getPartition(), currentOffset,
                       msgAndOffset.offset() + 1, message.checksum());

            etlKey.setMessageSize(msgAndOffset.message().size());

            currentOffset = msgAndOffset.offset() + 1; // increase offset
            currentCount++; // increase count

            return new KafkaMessage(payload, key, kafkaRequest.getTopic(),
                                    kafkaRequest.getPartition(),
                                    msgAndOffset.offset(), message.checksum());
        } else {
            return null;
        }
    }

    private byte[] getBytes(ByteBuffer buf) {
        if (buf == null) {
            return null;
        }
        int size = buf.remaining();
        byte[] bytes = new byte[size];
        buf.get(bytes, buf.position(), size);
        return bytes;
    }

    /**
     * Creates a fetch request.
     *
     * @return false if there's no more fetches
     */
    public boolean fetch() throws IOException, MetadataFetchException {
        if (currentOffset >= lastOffset) {
            return false;
        }
        long tempTime = System.currentTimeMillis();
        TopicAndPartition topicAndPartition = new TopicAndPartition(kafkaRequest.getTopic(),
                                                                    kafkaRequest.getPartition());
        log.debug("\nAsking for offset : " + (currentOffset));
        PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(currentOffset,
                                                                       fetchBufferSize);

        HashMap<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<>();
        fetchInfo.put(topicAndPartition, partitionFetchInfo);

        FetchRequest fetchRequest =
                new FetchRequest(CamusJob.getKafkaFetchRequestCorrelationId(context),
                                 CamusJob.getKafkaClientName(context),
                                 CamusJob.getKafkaFetchRequestMaxWait(context),
                                 CamusJob.getKafkaFetchRequestMinBytes(context), fetchInfo);

        FetchResponse fetchResponse;
        try {
            fetchResponse = simpleConsumer.fetch(fetchRequest);
            if (fetchResponse.hasError()) {
                short errorCode = fetchResponse.errorCode(kafkaRequest.getTopic(),
                                                          kafkaRequest.getPartition());
                String message = "Error Code generated : " + errorCode;
                throw new RuntimeException(message);
            }
            return processFetchResponse(fetchResponse, tempTime);
        } catch (Exception e) {
            log.info("Exception generated during fetch for topic " + kafkaRequest.getTopic() + ": "
                     + e.getMessage()
                     + "\n. Will refresh topic metadata and retry.");
            return refreshTopicMetadataAndRetryFetch(fetchRequest, tempTime);
        }
    }

    private boolean refreshTopicMetadataAndRetryFetch(FetchRequest fetchRequest, long tempTime)
            throws MetadataFetchException {
        try {
            refreshTopicMetadata();
            FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
            if (fetchResponse.hasError()) {
                log.warn("Error encountered during fetch request retry from Kafka");
                log.warn("Error Code generated : "
                         + fetchResponse
                                 .errorCode(kafkaRequest.getTopic(), kafkaRequest.getPartition()));
                return false;
            }
            return processFetchResponse(fetchResponse, tempTime);
        } catch (Exception e) {
            log.info("Exception generated during fetch for topic " + kafkaRequest.getTopic()
                     + ". Failing...");
            throw new MetadataFetchException(e);
        }
    }

    private void refreshTopicMetadata() {
        TopicMetadataRequest request = new TopicMetadataRequest(
                Collections.singletonList(kafkaRequest.getTopic()));
        TopicMetadataResponse response;
        try {
            response = simpleConsumer.send(request);
        } catch (Exception e) {
            log.error(
                    "Exception caught when refreshing metadata for topic " + request.topics().get(0)
                    + ": "
                    + e.getMessage());
            return;
        }
        TopicMetadata metadata = response.topicsMetadata().get(0);
        for (PartitionMetadata partitionMetadata : metadata.partitionsMetadata()) {
            if (partitionMetadata.partitionId() == kafkaRequest.getPartition()) {
                simpleConsumer =
                        new SimpleConsumer(partitionMetadata.leader().host(),
                                           partitionMetadata.leader().port(),
                                           CamusJob.getKafkaTimeoutValue(context),
                                           CamusJob.getKafkaBufferSize(context),
                                           CamusJob.getKafkaClientName(context));
                break;
            }
        }
    }

    private boolean processFetchResponse(FetchResponse fetchResponse, long tempTime) {
        try {
            ByteBufferMessageSet messageBuffer =
                    fetchResponse.messageSet(kafkaRequest.getTopic(), kafkaRequest.getPartition());
            lastFetchTime = (System.currentTimeMillis() - tempTime);
            log.debug("Time taken to fetch : " + (lastFetchTime / 1000) + " seconds");
            log.debug("The size of the ByteBufferMessageSet returned is : " + messageBuffer
                    .sizeInBytes());
            int skipped = 0;
            totalFetchTime += lastFetchTime;
            messageIter = messageBuffer.iterator();
            //boolean flag = false;
            Iterator<MessageAndOffset> messageIter2 = messageBuffer.iterator();
            MessageAndOffset message;
            while (messageIter2.hasNext()) {
                message = messageIter2.next();
                if (message.offset() < currentOffset) {
                    //flag = true;
                    skipped++;
                } else {
                    log.debug("Skipped offsets till : " + message.offset());
                    break;
                }
            }
            log.debug("Number of offsets to be skipped: " + skipped);
            while (skipped != 0) {
                MessageAndOffset skippedMessage = messageIter.next();
                log.debug("Skipping offset : " + skippedMessage.offset());
                skipped--;
            }

            if (!messageIter.hasNext()) {
                System.out.println("No more data left to process. Returning false");
                messageIter = null;
                return false;
            }

            return true;
        } catch (Exception e) {
            log.info("Exception generated during processing fetchResponse");
            return false;
        }
    }

    /**
     * Closes this context
     */
    public void close() throws IOException {
        if (simpleConsumer != null) {
            simpleConsumer.close();
        }
    }

    /**
     * Returns the total bytes that will be fetched. This is calculated by
     * taking the diffs of the offsets
     */
    public long getTotalBytes() {
        return (lastOffset > beginOffset) ? lastOffset - beginOffset : 0;
    }

    /**
     * Returns the total bytes that have been fetched so far
     */
    public long getReadBytes() {
        return currentOffset - beginOffset;
    }

    /**
     * Returns the number of events that have been read r
     */
    public long getCount() {
        return currentCount;
    }

    /**
     * Returns the fetch time of the last fetch in ms
     */
    public long getFetchTime() {
        return lastFetchTime;
    }

    /**
     * Returns the totalFetchTime in ms
     */
    public long getTotalFetchTime() {
        return totalFetchTime;
    }

    public static class MetadataFetchException extends Exception {

        public MetadataFetchException(Exception e) {
            super(e);
        }
    }
}
