package com.linkedin.camus.workallocater;

import org.apache.hadoop.io.Writable;

import java.net.URI;


public interface CamusRequest extends Writable {

    void setLatestOffset(long latestOffset);

    /**
     * Retrieve the topic
     */
    String getTopic();

    /**
     * Retrieves the uri if set. The default is null.
     */
    URI getURI();

    /**
     * Sets the broker uri for this request
     */
    void setURI(URI uri);

    /**
     * Retrieves the partition number
     */
    int getPartition();

    /**
     * Retrieves the offset
     */
    long getOffset();

    /**
     * Sets the starting offset used by the kafka pull mapper.
     */
    void setOffset(long offset);

    /**
     * Returns true if the offset is valid (>= to earliest offset && <= to last
     * offset)
     */
    boolean isValidOffset();

    long getEarliestOffset();

    void setEarliestOffset(long earliestOffset);

    long getLastOffset();

    long getLastOffset(long time);

    long estimateDataSize();

    void setAvgMsgSize(long size);

    /**
     * Estimates the request size in bytes by connecting to the broker and
     * querying for the offset that bets matches the endTime.
     *
     * @param endTime The time in millisec
     */
    long estimateDataSize(long endTime);

}
