package se.sics.anomaly.bs.models;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.windowing.time.Time;
import se.sics.anomaly.bs.core.PayloadFold;

/**
 * Created by mneumann on 2016-04-28.
 */
public class CountWindFold<V,K,RV> implements FoldFunction<V, Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>>, ResultTypeQueryable<Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>> {
        private PayloadFold<V,RV> plf;
        private KeySelector<V,K> kSelect;
        private double window;

        private transient TypeInformation<Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>> resultType;

        public CountWindFold(KeySelector<V, K> key, PayloadFold<V, RV> valueFold, Time window, TypeInformation<Tuple3<K, Tuple4<Double,Double,Long,Long>, RV>> resultType){
            this.plf = valueFold;
            this.kSelect = key;
            this.resultType = resultType;
            this.window = window.getSize();
        }

        @Override
        public TypeInformation<Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>> getProducedType() {
            return resultType;
        }

        @Override
        public Tuple3<K, Tuple4<Double,Double,Long,Long>, RV> fold(Tuple3<K, Tuple4<Double,Double,Long,Long>, RV> out, V value) throws Exception {

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

