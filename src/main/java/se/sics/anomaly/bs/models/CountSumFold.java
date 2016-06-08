package se.sics.anomaly.bs.models;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/**
 * Created by mneumann on 2016-04-28.
 */
public class CountSumFold<V,K> implements FoldFunction<V, Tuple2<K,Tuple4<Double,Double,Long,Long>>>, ResultTypeQueryable<Tuple2<K,Tuple4<Double,Double,Long,Long>>> {
        private KeySelector<V,K> kSelect;
        private KeySelector<V,Double> vSelect;

        private transient TypeInformation<Tuple2<K,Tuple4<Double,Double,Long,Long>>> resultType;

        public CountSumFold(KeySelector<V, K> key, KeySelector<V,Double> value, TypeInformation<Tuple2<K, Tuple4<Double, Double,Long,Long>>> resultType){
            this.kSelect = key;
            this.vSelect = value;
            this.resultType = resultType;
        }

        @Override
        public TypeInformation<Tuple2<K,Tuple4<Double,Double,Long,Long>>> getProducedType() {
            return resultType;
        }

        @Override
        public Tuple2<K, Tuple4<Double,Double,Long,Long>> fold(Tuple2<K, Tuple4<Double,Double,Long,Long>> out, V value) throws Exception {

            if (out.f0 == null){
                out.f0=kSelect.getKey(value);
            }

            out.f1.f0 +=1;
            out.f1.f1 += vSelect.getKey(value);

            return out;
        }
}

