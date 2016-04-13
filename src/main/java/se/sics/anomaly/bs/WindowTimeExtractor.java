package se.sics.anomaly.bs;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by mneumann on 2015-12-14.
 */
public class WindowTimeExtractor implements WindowFunction <Tuple2<String, Tuple2<Double, Double>>,Tuple2<String, Tuple4<Double, Double, Long,Long>>,Tuple,TimeWindow>{

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Tuple2<Double, Double>>> iterable, Collector<Tuple2<String, Tuple4<Double, Double, Long, Long>>> collector) throws Exception {
        Tuple2<String, Tuple2<Double, Double>> val = iterable.iterator().next();
        collector.collect(new Tuple2<>(val.f0,new Tuple4<>(val.f1.f0, val.f1.f1,timeWindow.getStart(),timeWindow.getEnd())));
    }
}
