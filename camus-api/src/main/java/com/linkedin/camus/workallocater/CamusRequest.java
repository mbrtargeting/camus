package com.linkedin.camus.workallocater;

import org.apache.hadoop.io.Writable;

import java.net.URI;


public interface CamusRequest extends Writable {

    public abstract void setLatestOffset(long latestOffset);

    /**
     * Retrieve the topic
     */
    public abstract String getTopic();

    /**
     * Retrieves the uri if set. The default is null.
     */
    public abstract URI getURI();

    /**
     * Sets the broker uri for this request
     */
    public abstract void setURI(URI uri);

    /**
     * Retrieves the partition number
     */
    public abstract int getPartition();

    /**
     * Retrieves the offset
     */
    public abstract long getOffset();

    /**
     * Sets the starting offset used by the kafka pull mapper.
     */
    public abstract void setOffset(long offset);

    /**
     * Returns true if the offset is valid (>= to earliest offset && <= to last
     * offset)
     */
    public abstract boolean isValidOffset();

    public abstract long getEarliestOffset();

    public abstract void setEarliestOffset(long earliestOffset);

    public abstract long getLastOffset();

    public abstract long getLastOffset(long time);

    public abstract long estimateDataSize();

    public abstract void setAvgMsgSize(long size);

    /**
     * Estimates the request size in bytes by connecting to the broker and
     * querying for the offset that bets matches the endTime.
     *
     * @param endTime The time in millisec
     */
    public abstract long estimateDataSize(long endTime);

}
