package se.sics.anomaly.bs.models;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import se.sics.anomaly.bs.core.PayloadFold;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by mneumann on 2016-04-28.
 */
public class CountSumFold2<V,RV> implements FoldFunction<V, Tuple3<String,Tuple2<Double,Double>, RV>> {
        private PayloadFold<V,RV> plf;
        private KeySelector<V,String> kSelect;
        private KeySelector<V,Double> vSelect;


        public CountSumFold2(KeySelector<V, String> key, KeySelector<V,Double> value, PayloadFold<V, RV> valueFold){
            this.plf = valueFold;
            this.kSelect = key;
            this.vSelect = value;
        }

        @Override
        public Tuple3<String, Tuple2<Double,Double>, RV> fold(Tuple3<String, Tuple2<Double,Double>, RV> out, V value) throws Exception {

            if (out.f0 != kSelect.getKey(value)){
                out.f0=kSelect.getKey(value);
            }

            RV plo = plf.fold(value, out.f2);
            out.f2 = plo;

            out.f1.f0 +=1;
            out.f1.f1 += vSelect.getKey(value);

            return out;
        }
}

