package se.sics.anomaly.bs.models;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.time.Time;
import se.sics.anomaly.bs.core.PayloadFold;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by mneumann on 2016-04-28.
 */
public class CountWindowFold<V,K,RV> implements FoldFunction<V, Tuple3<K,Tuple2<Double,Double>, RV>> {
    private PayloadFold<V,RV> plf;
    private KeySelector<V,K> kSelect;
    private double window;


    public CountWindowFold(KeySelector<V, K> key, PayloadFold<V, RV> valueFold, Time time){
        this.plf = valueFold;
        this.kSelect = key;
        this.window = time.getSize();
    }

    @Override
    public Tuple3<K, Tuple2<Double,Double>, RV> fold(Tuple3<K, Tuple2<Double,Double>, RV> out, V value) throws Exception {

        if (out.f0 != kSelect.getKey(value)){
            out.f0=kSelect.getKey(value);
        }

        RV plo = plf.fold(value, out.f2);
        out.f2 = plo;

        out.f1.f0 +=1;
        out.f1.f1 = window;

        return out;
    }
}
