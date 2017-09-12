package com.linkedin.camus.workallocater;

import com.linkedin.camus.etl.kafka.mapred.EtlSplit;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class BaseAllocator extends WorkAllocator {

    protected Properties props;

    public void init(Properties props) {
        this.props = props;
    }

    @Override
    public List<InputSplit> allocateWork(List<CamusRequest> requests, JobContext context)
            throws IOException {
        int numTasks = context.getConfiguration().getInt("mapred.map.tasks", 30);

        requests.sort((r1, r2) -> r1.estimateDataSize() > r2.estimateDataSize() ? -1 : 1); // reverse sort

        List<InputSplit> kafkaETLSplits = new ArrayList<>();

        if (requests.size() > 0) {
            for (int i = 0; i < numTasks; i++) {
                kafkaETLSplits.add(new EtlSplit());
            }
        }

        for (CamusRequest r : requests) {
            getSmallestMultiSplit(kafkaETLSplits).addRequest(r);
        }

        return kafkaETLSplits;
    }

    protected EtlSplit getSmallestMultiSplit(List<InputSplit> kafkaETLSplits) throws IOException {
        EtlSplit smallest = (EtlSplit) kafkaETLSplits.get(0);

        for (int i = 1; i < kafkaETLSplits.size(); i++) {
            EtlSplit challenger = (EtlSplit) kafkaETLSplits.get(i);
            if ((smallest.getLength() == challenger.getLength()
                 && smallest.getNumRequests() > challenger.getNumRequests())
                || smallest.getLength() > challenger.getLength()) {
                smallest = challenger;
            }
        }

        return smallest;
    }

}
