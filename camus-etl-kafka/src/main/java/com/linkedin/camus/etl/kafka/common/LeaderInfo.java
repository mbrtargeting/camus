package com.linkedin.camus.etl.kafka.common;

import java.net.URI;


/**
 * Model class to store the leaderInformation
 *
 * @author ggupta
 */

public class LeaderInfo {

    private final URI uri;
    private final int leaderId;

    public LeaderInfo(URI uri, int leaderId) {
        this.uri = uri;
        this.leaderId = leaderId;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public URI getUri() {
        return uri;
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LeaderInfo && this.hashCode() == obj.hashCode();
    }
}
