package com.linkedin.camus.callback;


import org.apache.hadoop.mapreduce.Job;

public interface Callback {

    void onSuccess(final Job job);

    void onFailure(final Job job);
}
