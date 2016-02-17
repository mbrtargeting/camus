package com.linkedin.camus.etl.kafka.common;

/**
 * Created by michaelandrepearce on 05/04/15.
 */
public class KafkaMessage implements com.linkedin.camus.coders.Message {

    final byte[] payload;
    final byte[] key;

    private String topic = "";
    private long offset = 0;
    private int partition = 0;
    private long checksum = 0;


    public KafkaMessage(byte[] payload, byte[] key, String topic, int partition, long offset,
                        long checksum) {
        this.payload = payload;
        this.key = key;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.checksum = checksum;
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public int getPartition() {
        return partition;
    }

    @Override
    public long getChecksum() {
        return checksum;
    }
}
