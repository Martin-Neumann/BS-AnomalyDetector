package se.sics.anomaly.bs.models.normal;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import se.sics.anomaly.bs.core.AnomalyResult;
import se.sics.anomaly.bs.core.KeyedAnomalyFlatMap;
import se.sics.anomaly.bs.core.PayloadFold;
import se.sics.anomaly.bs.core.WindowTimeExtractor;
import se.sics.anomaly.bs.history.History;
import se.sics.anomaly.bs.models.CountSumFold;
import se.sics.anomaly.bs.models.CountWindFold;

/**
 * Created by mneumann on 2016-04-27.
 */
public class NormalFreqAnomaly<K,V> {
    private KeyedAnomalyFlatMap<K,NormalModel> afm;

    public NormalFreqAnomaly(boolean addIfAnomaly, double anomalyLevel, History hist){
        this.afm = new KeyedAnomalyFlatMap<>(14d,new NormalModel(hist), true);
    }

    public NormalFreqAnomaly(History hist){
        new NormalFreqAnomaly(false,14d,hist);
    }

    public DataStream<Tuple2<K, AnomalyResult>> getAnomalySteam(DataStream<V> ds, KeySelector<V, K> keySelector , KeySelector<V,Double> valueSelector,  Time window) {

        KeyedStream<V, K> keyedInput = ds
                .keyBy(keySelector);

         TypeInformation<Tuple2<K,Tuple4<Double,Double,Long,Long>>> resultType = (TypeInformation) new TupleTypeInfo<>(Tuple2.class,
                new TypeInformation[] {keyedInput.getKeyType(), new TupleTypeInfo(Tuple4.class,
                        BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,BasicTypeInfo.LONG_TYPE_INFO)});

        Tuple2<K,Tuple4<Double,Double,Long,Long>> init= new Tuple2<>(null,new Tuple4<>(0d,0d,0l,0l));
        KeyedStream<Tuple2<K,Tuple4<Double,Double,Long,Long>>, Tuple> kPreStream = keyedInput
                .timeWindow(window)
                .apply(init, new CountWindFold<>(keySelector, window, resultType),new WindowTimeExtractor(resultType))
                .keyBy(0);

        return kPreStream.flatMap(afm);
    }



}
