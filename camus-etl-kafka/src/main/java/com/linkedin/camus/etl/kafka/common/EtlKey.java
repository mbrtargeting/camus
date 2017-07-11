package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.etl.IEtlKey;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;


/**
 * The key for the mapreduce job to pull kafka. Contains offsets and the
 * checksum.
 */
public class EtlKey implements WritableComparable<EtlKey>, IEtlKey {

    public static final Text SERVER = new Text("server");
    public static final Text SERVICE = new Text("service");

    private String leaderId = "";
    private int partition = 0;
    private long beginOffset = 0;
    private long offset = 0;
    private long checksum = 0;
    private String topic = "";
    private long time = 0;
    private String server = "";
    private String service = "";
    private long totalMessageSize = 0;
    private MapWritable partitionMap = new MapWritable();

    /**
     * dummy empty constructor
     */
    public EtlKey() {
        this("dummy", "0", 0, 0, 0, 0);
    }

    public EtlKey(EtlKey other) {

        this.partition = other.partition;
        this.beginOffset = other.beginOffset;
        this.offset = other.offset;
        this.checksum = other.checksum;
        this.topic = other.topic;
        this.time = other.time;
        this.server = other.server;
        this.service = other.service;
        this.partitionMap = new MapWritable(other.partitionMap);
    }

    public EtlKey(String topic, String leaderId, int partition) {
        set(topic, leaderId, partition, 0, 0, 0);
    }

    public EtlKey(String topic, String leaderId, int partition, long beginOffset, long offset) {
        set(topic, leaderId, partition, beginOffset, offset, 0);
    }

    public EtlKey(String topic, String leaderId, int partition, long beginOffset, long offset,
                  long checksum) {
        set(topic, leaderId, partition, beginOffset, offset, checksum);
    }

    public void set(String topic, String leaderId, int partition, long beginOffset, long offset,
                    long checksum) {
        this.leaderId = leaderId;
        this.partition = partition;
        this.beginOffset = beginOffset;
        this.offset = offset;
        this.checksum = checksum;
        this.topic = topic;
        this.time = System.currentTimeMillis(); // if event can't be decoded,
        // this time will be used for
        // debugging.
    }

    public void clear() {
        leaderId = "";
        partition = 0;
        beginOffset = 0;
        offset = 0;
        checksum = 0;
        topic = "";
        time = 0;
        server = "";
        service = "";
        partitionMap = new MapWritable();
    }

    public String getServer() {
        return partitionMap.get(SERVER).toString();
    }

    public void setServer(String newServer) {
        partitionMap.put(SERVER, new Text(newServer));
    }

    public String getService() {
        return partitionMap.get(SERVICE).toString();
    }

    public void setService(String newService) {
        partitionMap.put(SERVICE, new Text(newService));
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getTopic() {
        return topic;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public int getPartition() {
        return partition;
    }

    public long getBeginOffset() {
        return beginOffset;
    }

    public long getOffset() {
        return offset;
    }

    public long getChecksum() {
        return checksum;
    }

    @Override
    public long getMessageSize() {
        Text key = new Text("message.size");
        if (partitionMap.containsKey(key)) {
            return ((LongWritable) partitionMap.get(key)).get();
        } else {
            return 1024; //default estimated size
        }
    }

    public void setMessageSize(long messageSize) {
        Text key = new Text("message.size");
        put(key, new LongWritable(messageSize));
    }

    public long getTotalMessageSize() {
        if (totalMessageSize == 0) {
            totalMessageSize = getMessageSize();
        }
        return totalMessageSize;
    }

    public void setTotalMessageSize(long totalMessageSize) {
        this.totalMessageSize = totalMessageSize;
    }

    public void put(Writable key, Writable value) {
        partitionMap.put(key, value);
    }

    public void addAllPartitionMap(MapWritable partitionMap) {
        this.partitionMap.putAll(partitionMap);
    }

    public MapWritable getPartitionMap() {
        return partitionMap;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        leaderId = UTF8.readString(in);
        partition = in.readInt();
        beginOffset = in.readLong();
        offset = in.readLong();
        checksum = in.readLong();
        topic = in.readUTF();
        time = in.readLong();
        server = in.readUTF(); // left for legacy
        service = in.readUTF(); // left for legacy
        partitionMap = new MapWritable();
        try {
            partitionMap.readFields(in);
        } catch (IOException e) {
            setServer(server);
            setService(service);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        UTF8.writeString(out, leaderId);
        out.writeInt(partition);
        out.writeLong(beginOffset);
        out.writeLong(offset);
        out.writeLong(checksum);
        out.writeUTF(topic);
        out.writeLong(time);
        out.writeUTF(server); // left for legacy
        out.writeUTF(service); // left for legacy
        partitionMap.write(out);
    }

    @Override
    public int compareTo(EtlKey o) {
        if (partition != o.partition) {
            return partition = o.partition;
        } else {
            if (offset > o.offset) {
                return 1;
            } else if (offset < o.offset) {
                return -1;
            } else {
                if (checksum > o.checksum) {
                    return 1;
                } else if (checksum < o.checksum) {
                    return -1;
                } else {
                    return 0;
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("topic=");
        builder.append(topic);
        builder.append(" partition=");
        builder.append(partition);
        builder.append("leaderId=");
        builder.append(leaderId);
        builder.append(" server=");
        builder.append(server);
        builder.append(" service=");
        builder.append(service);
        builder.append(" beginOffset=");
        builder.append(beginOffset);
        builder.append(" offset=");
        builder.append(offset);
        builder.append(" msgSize=");
        builder.append(getMessageSize());
        builder.append(" server=");
        builder.append(server);
        builder.append(" checksum=");
        builder.append(checksum);
        builder.append(" time=");
        builder.append(time);

        for (Map.Entry<Writable, Writable> e : partitionMap.entrySet()) {
            builder.append(" ").append(e.getKey()).append("=");
            builder.append(e.getValue().toString());
        }

        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EtlKey)) {
            return false;
        }

        EtlKey etlKey = (EtlKey) o;

        return beginOffset == etlKey.beginOffset
               && checksum == etlKey.checksum
               && offset == etlKey.offset
               && partition == etlKey.partition
               && time == etlKey.time
               && leaderId.equals(etlKey.leaderId)
               && partitionMap.equals(etlKey.partitionMap)
               && server.equals(etlKey.server)
               && service.equals(etlKey.service)
               && topic.equals(etlKey.topic);
    }

    @Override
    public int hashCode() {
        int result = leaderId.hashCode();
        result = 31 * result + partition;
        result = 31 * result + (int) (beginOffset ^ (beginOffset >>> 32));
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + (int) (checksum ^ (checksum >>> 32));
        result = 31 * result + topic.hashCode();
        result = 31 * result + (int) (time ^ (time >>> 32));
        result = 31 * result + server.hashCode();
        result = 31 * result + service.hashCode();
        result = 31 * result + partitionMap.hashCode();
        return result;
    }
}
