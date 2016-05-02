package se.sics.anomaly.bs.models.poisson;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import se.sics.anomaly.bs.core.AnomalyResult;
import se.sics.anomaly.bs.core.KeyedAnomalyFlatMap;
import se.sics.anomaly.bs.core.PayloadFold;
import se.sics.anomaly.bs.history.History;
import se.sics.anomaly.bs.models.CountWindowFold;

/**
 * Created by mneumann on 2016-04-27.
 */
public class PoissonFreqAnomaly<K,V,RV> {
    private KeyedAnomalyFlatMap<K,PoissonModel,RV> afm;

    public PoissonFreqAnomaly(boolean addIfAnomaly, double anomalyLevel, History hist){
        this.afm = new KeyedAnomalyFlatMap<>(14d,new PoissonModel(hist), true);
    }

    public PoissonFreqAnomaly(History hist){
        new PoissonFreqAnomaly(false,14d,hist);
    }

    public DataStream<Tuple3<K, AnomalyResult, RV>> getAnomalySteam(DataStream<V> ds, KeySelector<V, K> keySelector, K keyInit , PayloadFold<V, RV> valueFold, Time window) {
        KeyedStream<Tuple3<K,Tuple2<Double,Double>,RV>, Tuple> kPreStream = ds
                .keyBy(keySelector)
                .timeWindow(window)
                .fold((new Tuple3<>(keyInit ,new Tuple2<Double,Double>(0d,0d), valueFold.getInit())), new CountWindowFold<>(keySelector,valueFold, window))
                .keyBy(0);
        return kPreStream.flatMap(afm);
    }
}