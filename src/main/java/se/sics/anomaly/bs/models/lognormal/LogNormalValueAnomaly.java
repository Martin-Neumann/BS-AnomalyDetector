package se.sics.anomaly.bs.models.lognormal;

import org.apache.flink.api.common.functions.FoldFunction;
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
import se.sics.anomaly.bs.models.normal.NormalModel;
import se.sics.anomaly.bs.models.poisson.PoissonModel;

/**
 * Created by mneumann on 2016-04-27.
 */
public class LogNormalValueAnomaly<K,V,RV> {

    private KeyedAnomalyFlatMap<K,NormalModel,RV> afm;

    public LogNormalValueAnomaly(boolean addIfAnomaly, double anomalyLevel, History hist){
        this.afm = new KeyedAnomalyFlatMap<>(14d,new NormalModel(hist), true);
    }

    public LogNormalValueAnomaly(History hist){
        new LogNormalValueAnomaly(false,14d,hist);
    }
    public DataStream<Tuple3<K, AnomalyResult, RV>> getAnomalySteam(DataStream<V> ds, KeySelector<V, K> keySelector, K keyInit , KeySelector<V,Double> valueSelector, PayloadFold<V, RV> valueFold, Time window) {
        KeyedStream<Tuple3<K,Tuple2<Double,Double>,RV>, Tuple> kPreStream = ds
                .keyBy(keySelector)
                .timeWindow(window)
                .fold((new Tuple3<>(keyInit ,new Tuple2<>(0d,0d), valueFold.getInit())), new LogCountSumFold<>(keySelector,valueSelector,valueFold))
                .keyBy(0);
        return kPreStream.flatMap(afm);
    }


    private class LogCountSumFold<V,K,RV> implements FoldFunction<V, Tuple3<K,Tuple2<Double,Double>, RV>> {
        private PayloadFold<V,RV> plf;
        private KeySelector<V,K> kSelect;
        private KeySelector<V,Double> vSelect;


        public LogCountSumFold(KeySelector<V, K> key, KeySelector<V,Double> value, PayloadFold<V, RV> valueFold){
            this.plf = valueFold;
            this.kSelect = key;
            this.vSelect = value;
        }

        @Override
        public Tuple3<K, Tuple2<Double,Double>, RV> fold(Tuple3<K, Tuple2<Double,Double>, RV> out, V value) throws Exception {

            if (out.f0 != kSelect.getKey(value)){
                out.f0=kSelect.getKey(value);
            }

            RV plo = plf.fold(value, out.f2);
            out.f2 = plo;

            out.f1.f0 +=1;
            out.f1.f1 += Math.log(vSelect.getKey(value));

            return out;
        }
    }
}