package se.sics.anomaly.bs.core;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by mneumann on 2016-05-12.
 */
public class WindowTimeExtractor<K> implements WindowFunction<Tuple2<K,Tuple4<Double,Double,Long,Long>>,Tuple2<K,Tuple4<Double,Double,Long,Long>>,K,TimeWindow> ,ResultTypeQueryable<Tuple2<K,Tuple4<Double,Double,Long,Long>>> {
    private transient TypeInformation<Tuple2<K,Tuple4<Double,Double,Long,Long>>> resultType;

    public WindowTimeExtractor (TypeInformation<Tuple2<K, Tuple4<Double, Double,Long,Long>>> resultType){
        this.resultType = resultType;
    }

    @Override
    public void apply(K key, TimeWindow timeWindow, Iterable<Tuple2<K, Tuple4<Double, Double,Long,Long>>> iterable, Collector<Tuple2<K, Tuple4<Double, Double, Long, Long>>> collector) throws Exception {
        Tuple2<K,Tuple4<Double,Double,Long,Long>> out = iterable.iterator().next();
        out.f1.f2 = timeWindow.getStart();
        out.f1.f3 = timeWindow.getEnd();
        collector.collect(out);
    }

    @Override
    public TypeInformation<Tuple2<K, Tuple4<Double, Double,Long,Long>>> getProducedType() {
        return resultType;
    }
}
