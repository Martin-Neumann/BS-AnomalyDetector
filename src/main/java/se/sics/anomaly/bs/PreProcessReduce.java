package se.sics.anomaly.bs;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by mneumann on 2015-12-14.
 */
public class PreProcessReduce implements ReduceFunction<Tuple2<String, Tuple2<Double, Double>>> {

    @Override
    public Tuple2<String, Tuple2<Double, Double>> reduce(Tuple2<String, Tuple2<Double, Double>> t1, Tuple2<String, Tuple2<Double, Double>> t2) throws Exception {
        t1.f1.f0 +=t2.f1.f0;
        t1.f1.f1 +=t2.f1.f1;
        return t1;
    }
}