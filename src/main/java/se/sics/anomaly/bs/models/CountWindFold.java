package se.sics.anomaly.bs.models;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by mneumann on 2016-04-28.
 */
public class CountWindFold<V,K> implements FoldFunction<V, Tuple2<K,Tuple4<Double,Double,Long,Long>>>, ResultTypeQueryable<Tuple2<K,Tuple4<Double,Double,Long,Long>>> {
        private KeySelector<V,K> kSelect;
        private double window;

        private transient TypeInformation<Tuple2<K,Tuple4<Double,Double,Long,Long>>> resultType;

        public CountWindFold(KeySelector<V, K> key, Time window, TypeInformation<Tuple2<K, Tuple4<Double,Double,Long,Long>>> resultType){
            this.kSelect = key;
            this.resultType = resultType;
            this.window = window.getSize();
        }

        @Override
        public TypeInformation<Tuple2<K,Tuple4<Double,Double,Long,Long>>> getProducedType() {
            return resultType;
        }

        @Override
        public Tuple2<K, Tuple4<Double,Double,Long,Long>> fold(Tuple2<K, Tuple4<Double,Double,Long,Long>> out, V value) throws Exception {

            if (out.f0 != kSelect.getKey(value)){
                out.f0=kSelect.getKey(value);
            }

            out.f1.f0 +=1;
            out.f1.f1 = window;

            return out;
        }
}

