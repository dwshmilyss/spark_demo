package com.cloudera.livy.examples;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;
import java.util.ArrayList;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class PiJob implements Job<Double>, Function<Integer, Integer>, Function2<Integer, Integer, Integer> {
    private final int slices;
    private final int samples;

    public PiJob(int slices) {
        this.slices = slices;
        this.samples = (int)Math.min(100000L * (long)slices, 2147483647L);
    }

    public Double call(JobContext ctx) throws Exception {
        ArrayList sampleList = new ArrayList();

        for(int i = 0; i < this.samples; ++i) {
            sampleList.add(Integer.valueOf(i));
        }

        return Double.valueOf(4.0D * (double)((Integer)ctx.sc().parallelize(sampleList, this.slices).map(this).reduce(this)).intValue() / (double)this.samples);
    }

    public Integer call(Integer v1) {
        double x = Math.random() * 2.0D - 1.0D;
        double y = Math.random() * 2.0D - 1.0D;
        return Integer.valueOf(x * x + y * y < 1.0D?1:0);
    }

    public Integer call(Integer v1, Integer v2) {
        return Integer.valueOf(v1.intValue() + v2.intValue());
    }
}