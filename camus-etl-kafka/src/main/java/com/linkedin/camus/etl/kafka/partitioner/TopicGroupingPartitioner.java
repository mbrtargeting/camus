package com.linkedin.camus.etl.kafka.partitioner;

public class TopicGroupingPartitioner extends DefaultPartitioner {

    @Override
    public String getWorkingFileName(String topic, String brokerId,
                                     int partitionId,
                                     String encodedPartition) {

        return super.getWorkingFileName(topic, "0", 0, encodedPartition);
    }

    @Override
    public String generateFileName(int count, long offset) {

        return super.generateFileName(count, 0);
    }

}
