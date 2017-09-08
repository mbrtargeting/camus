package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.workallocater.CamusRequest;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;


public class EtlSplit extends InputSplit implements Writable {

    // Sort by topic, so that requests from the same topic are processed after each other
    private final Comparator<CamusRequest> priorityComparator = Comparator.comparing(CamusRequest::getTopic);

    private final Queue<CamusRequest> requests = new PriorityQueue<>(5, priorityComparator);
    private long length = 0;

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            CamusRequest r = new EtlRequest();
            r.readFields(in);
            addRequest(r);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(requests.size());
        for (CamusRequest r : requests) {
            r.write(out);
        }
    }

    @Override
    public long getLength() throws IOException {
        return length;
    }

    public int getNumRequests() {
        return requests.size();
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[]{};
    }

    public void addRequest(CamusRequest request) {
        requests.add(request);
        length += request.estimateDataSize();
    }

    public CamusRequest popRequest() {
        return requests.poll();
    }
}
