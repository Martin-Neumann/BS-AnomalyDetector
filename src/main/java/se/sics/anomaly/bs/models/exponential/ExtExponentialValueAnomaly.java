package se.sics.anomaly.bs.models.exponential;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import se.sics.anomaly.bs.core.*;
import se.sics.anomaly.bs.history.History;
import se.sics.anomaly.bs.models.CountSumFold;
import se.sics.anomaly.bs.models.ExtCountSumFold;
import se.sics.anomaly.bs.models.poisson.PoissonModel;

/**
 * Created by mneumann on 2016-04-27.
 */
public class ExtExponentialValueAnomaly<K,V,RV> {

    private ExtKeyedAnomalyFlatMap<K,PoissonModel,RV> afm;

    public ExtExponentialValueAnomaly(boolean addIfAnomaly, double anomalyLevel, History hist){
        this.afm = new ExtKeyedAnomalyFlatMap<>(14d,new PoissonModel(hist), true);
    }

    public ExtExponentialValueAnomaly(History hist){
        this(false,14d,hist);
    }

    public DataStream<Tuple3<K, AnomalyResult, RV>> getAnomalySteam(DataStream<V> ds, KeySelector<V, K> keySelector, KeySelector<V,Double> valueSelector, PayloadFold<V, RV> valueFold, Time window) {

        KeyedStream<V, K> keyedInput = ds
                .keyBy(keySelector);

        TypeInformation<Object> foldResultType = TypeExtractor.getUnaryOperatorReturnType(valueFold,
                PayloadFold.class,
                false,
                false,
                ds.getType(),
                "PayloadFold",
                false);

        TypeInformation<Tuple3<K,Tuple4<Double,Double,Long,Long>,RV>> resultType = (TypeInformation) new TupleTypeInfo<>(Tuple3.class,
                new TypeInformation[] {keyedInput.getKeyType(), new TupleTypeInfo(Tuple4.class,
                        BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,BasicTypeInfo.LONG_TYPE_INFO), foldResultType});

        Tuple3<K,Tuple4<Double,Double,Long,Long>, RV> init= new Tuple3<>(null,new Tuple4<>(0d,0d,0l,0l), valueFold.getInit());
        KeyedStream<Tuple3<K,Tuple4<Double,Double,Long,Long>,RV>, Tuple> kPreStream = keyedInput
                .timeWindow(window)
                .apply(init, new ExtCountSumFold<>(keySelector,valueSelector,valueFold, resultType),new ExtWindowTimeExtractor<>(resultType))
                .keyBy(0);

        return kPreStream.flatMap(afm);
    }
}
