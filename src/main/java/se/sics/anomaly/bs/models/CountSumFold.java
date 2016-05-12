package se.sics.anomaly.bs.models;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import se.sics.anomaly.bs.core.PayloadFold;

/**
 * Created by mneumann on 2016-04-28.
 */
public class CountSumFold<V,K,RV> implements FoldFunction<V, Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>>, ResultTypeQueryable<Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>> {
        private PayloadFold<V,RV> plf;
        private KeySelector<V,K> kSelect;
        private KeySelector<V,Double> vSelect;

        private transient TypeInformation<Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>> resultType;

        public CountSumFold(KeySelector<V, K> key, KeySelector<V,Double> value, PayloadFold<V, RV> valueFold, TypeInformation<Tuple3<K, Tuple4<Double, Double,Long,Long>, RV>> resultType){
            this.plf = valueFold;
            this.kSelect = key;
            this.vSelect = value;
            this.resultType = resultType;
        }

        @Override
        public TypeInformation<Tuple3<K,Tuple4<Double,Double,Long,Long>, RV>> getProducedType() {
            return resultType;
        }

        @Override
        public Tuple3<K, Tuple4<Double,Double,Long,Long>, RV> fold(Tuple3<K, Tuple4<Double,Double,Long,Long>, RV> out, V value) throws Exception {

            if (out.f0 == null){
                out.f0=kSelect.getKey(value);
            }

            RV plo = plf.fold(value, out.f2);
            out.f2 = plo;

            out.f1.f0 +=1;
            out.f1.f1 += vSelect.getKey(value);

            return out;
        }
}

