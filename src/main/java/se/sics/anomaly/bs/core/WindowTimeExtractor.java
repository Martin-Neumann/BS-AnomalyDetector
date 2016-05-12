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
public class WindowTimeExtractor<K,RV> implements WindowFunction<Tuple3<K,Tuple2<Double,Double>, RV>,Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>,Tuple,TimeWindow> ,ResultTypeQueryable<Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>> {
    private transient TypeInformation<Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>> resultType;

    public WindowTimeExtractor (TypeInformation<Tuple3<K, Tuple4<Double, Double,Long,Long>, RV>> resultType){
        this.resultType = resultType;
    }

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<K, Tuple2<Double, Double>, RV>> iterable, Collector<Tuple3<K, Tuple4<Double, Double, Long, Long>, RV>> collector) throws Exception {
        Tuple3<K,Tuple2<Double,Double>, RV> val = iterable.iterator().next();
        Tuple3<K,Tuple4<Double,Double,Long,Long>, RV> out = new Tuple3(val.f0,new Tuple4<>(val.f1.f0,val.f1.f1,timeWindow.getStart(),timeWindow.getEnd()),val.f2);
        collector.collect(out);
    }

    @Override
    public TypeInformation<Tuple3<K, Tuple4<Double, Double,Long,Long>, RV>> getProducedType() {
        return resultType;
    }
}
