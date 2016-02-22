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
import kafka.common.ErrorMapping;
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
    private final static Logger log = Logger.getLogger(KafkaReader.class);
    private final EtlRequest kafkaRequest;
    private SimpleConsumer simpleConsumer;

    private final long beginOffset;
    private final long lastOffset;
    private long currentOffset;
    private long currentCount;

    private final TaskAttemptContext context;

    private Iterator<MessageAndOffset> messageIter = null;

    private long totalFetchTime = 0;
    private long lastFetchTime = 0;

    private final int fetchBufferSize;

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
        log.info(String.format(
                "Connected to leader %s beginning reading at offset %d latest offset=%d", uri,
                beginOffset, lastOffset));
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
    public KafkaMessage getNext(final EtlKey etlKey) throws IOException, MetadataFetchException {
        if (!hasNext()) {
            return null;
        }
        final MessageAndOffset msgAndOffset = messageIter.next();
        final Message message = msgAndOffset.message();

        byte[] payload = getBytes(message.payload());
        byte[] key = getBytes(message.key());

        if (payload == null) {
            log.warn("Received message with null message.payload(): " + msgAndOffset);
        }

        etlKey.clear();
        etlKey.set(kafkaRequest.getTopic(), kafkaRequest.getLeaderId(), kafkaRequest.getPartition(),
                   currentOffset, msgAndOffset.offset() + 1, message.checksum());

        etlKey.setMessageSize(msgAndOffset.message().size());

        currentOffset = msgAndOffset.offset() + 1; // increase offset
        currentCount++; // increase count

        return new KafkaMessage(payload, key, kafkaRequest.getTopic(), kafkaRequest.getPartition(),
                                msgAndOffset.offset(), message.checksum());
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
        final TopicAndPartition topicAndPartition = new TopicAndPartition(
                kafkaRequest.getTopic(),
                kafkaRequest.getPartition());
        log.debug("\nAsking for offset : " + currentOffset);
        final PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(currentOffset,
                                                                             fetchBufferSize);

        final HashMap<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<>();
        fetchInfo.put(topicAndPartition, partitionFetchInfo);

        FetchRequest fetchRequest =
                new FetchRequest(CamusJob.getKafkaFetchRequestCorrelationId(context),
                                 CamusJob.getKafkaClientName(context),
                                 CamusJob.getKafkaFetchRequestMaxWait(context),
                                 CamusJob.getKafkaFetchRequestMinBytes(context), fetchInfo);

        try {
            final FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
            if (fetchResponse.hasError()) {
                final Throwable cause = ErrorMapping.exceptionFor(
                        fetchResponse.errorCode(kafkaRequest.getTopic(),
                                                kafkaRequest.getPartition()));
                throw new RuntimeException("Fetch Error", cause);
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
            final FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
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
        final TopicMetadataRequest request = new TopicMetadataRequest(
                Collections.singletonList(kafkaRequest.getTopic()));
        final TopicMetadataResponse response;
        try {
            response = simpleConsumer.send(request);
        } catch (Exception e) {
            log.error(String.format("Exception caught when refreshing metadata for topic %s: %s",
                                    request.topics().get(0), e.getMessage()));
            return;
        }
        final TopicMetadata metadata = response.topicsMetadata().get(0);
        for (final PartitionMetadata partitionMetadata : metadata.partitionsMetadata()) {
            if (partitionMetadata.partitionId() == kafkaRequest.getPartition()) {
                simpleConsumer = new SimpleConsumer(partitionMetadata.leader().host(),
                                                    partitionMetadata.leader().port(),
                                                    CamusJob.getKafkaTimeoutValue(context),
                                                    CamusJob.getKafkaBufferSize(context),
                                                    CamusJob.getKafkaClientName(context));
                return;
            }
        }
    }

    private boolean processFetchResponse(FetchResponse fetchResponse, long tempTime) {
        try {
            final ByteBufferMessageSet messageBuffer =
                    fetchResponse.messageSet(kafkaRequest.getTopic(), kafkaRequest.getPartition());
            lastFetchTime = System.currentTimeMillis() - tempTime;
            log.debug("Time taken to fetch : " + (lastFetchTime / 1000) + " seconds");
            log.debug("The size of the ByteBufferMessageSet returned is : " + messageBuffer
                    .sizeInBytes());
            totalFetchTime += lastFetchTime;
            messageIter = messageBuffer.iterator();
            final Iterator<MessageAndOffset> lookAhead = messageBuffer.iterator();
            MessageAndOffset message;
            int skipped = 0;
            while (lookAhead.hasNext()) {
                message = lookAhead.next();
                if (message.offset() < currentOffset) {
                    skipped++;
                    messageIter.next();
                } else {
                    log.debug("Number of offsets to be skipped: " + skipped);
                    log.debug("Skipped offsets till : " + message.offset());
                    break;
                }
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
