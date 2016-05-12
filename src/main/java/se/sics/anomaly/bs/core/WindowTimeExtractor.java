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
public class WindowTimeExtractor<K,RV> implements WindowFunction<Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>,Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>,K,TimeWindow> ,ResultTypeQueryable<Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>> {
    private transient TypeInformation<Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>> resultType;

    public WindowTimeExtractor (TypeInformation<Tuple3<K, Tuple4<Double, Double,Long,Long>, RV>> resultType){
        this.resultType = resultType;
    }

    @Override
    public void apply(K key, TimeWindow timeWindow, Iterable<Tuple3<K, Tuple4<Double, Double,Long,Long>, RV>> iterable, Collector<Tuple3<K, Tuple4<Double, Double, Long, Long>, RV>> collector) throws Exception {
        Tuple3<K,Tuple4<Double,Double,Long,Long>, RV> out = iterable.iterator().next();
        out.f1.f2 = timeWindow.getStart();
        out.f1.f3 = timeWindow.getEnd();
        collector.collect(out);
    }

    @Override
    public TypeInformation<Tuple3<K, Tuple4<Double, Double,Long,Long>, RV>> getProducedType() {
        return resultType;
    }
}
