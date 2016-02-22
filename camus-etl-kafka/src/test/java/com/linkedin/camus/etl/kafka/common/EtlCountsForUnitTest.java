package com.linkedin.camus.etl.kafka.common;

public class EtlCountsForUnitTest extends EtlCounts {

    public static ProducerType producerType = ProducerType.REGULAR;

    public EtlCountsForUnitTest(EtlCounts other) {
        super(other.getTopic(), other.getGranularity(), other.getStartTime());
        this.counts = other.getCounts();
    }

    public static void reset() {
        producerType = ProducerType.REGULAR;
    }

    public enum ProducerType {
        REGULAR,
        SEND_THROWS_EXCEPTION,
        SEND_SUCCEED_THIRD_TIME
    }
}
