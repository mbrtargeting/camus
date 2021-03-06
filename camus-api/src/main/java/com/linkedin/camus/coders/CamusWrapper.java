package com.linkedin.camus.coders;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Container for messages.  Enables the use of a custom message decoder with knowledge
 * of where these values are stored in the message schema
 *
 * @param <R> The type of decoded payload
 * @author kgoodhop
 */
public class CamusWrapper<R> {

    private final R record;
    private final long timestamp;
    private final MapWritable partitionMap;

    public CamusWrapper(R record) {
        this(record, System.currentTimeMillis());
    }

    public CamusWrapper(R record, long timestamp) {
        this(record, timestamp, "unknown_server", "unknown_service");
    }

    public CamusWrapper(R record, long timestamp, String server, String service) {
        this.record = record;
        this.timestamp = timestamp;
        partitionMap = new MapWritable();
        partitionMap.put(new Text("server"), new Text(server));
        partitionMap.put(new Text("service"), new Text(service));
    }

    /**
     * Returns the payload record for a single message
     */
    public R getRecord() {
        return record;
    }

    /**
     * Returns current if not set by the decoder
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Add a value for partitions
     */
    public void put(Writable key, Writable value) {
        partitionMap.put(key, value);
    }

    /**
     * Get a value for partitions
     *
     * @return the value for the given key
     */
    public Writable get(Writable key) {
        return partitionMap.get(key);
    }

    /**
     * Get all the partition key/partitionMap
     */
    public MapWritable getPartitionMap() {
        return partitionMap;
    }

}
