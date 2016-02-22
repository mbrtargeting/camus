package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.common.EtlKey;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * KafkaETL mapper
 *
 * input -- EtlKey
 *
 * output -- EtlKey
 */
public class EtlMapper extends Mapper<EtlKey, CamusWrapper, EtlKey, CamusWrapper> {

    @Override
    public void map(EtlKey key, CamusWrapper val, Context context)
            throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();

        context.write(key, val);

        long endTime = System.currentTimeMillis();
        long mapTime = endTime - startTime;
        context.getCounter("total", "mapper-time(ms)").increment(mapTime);
    }
}
