package se.sics.anomaly.bs.models.lognormal;

import org.apache.flink.api.common.functions.FoldFunction;
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

        TypeInformation<Tuple3<K,Tuple2<Double,Double>,RV>> resultType = (TypeInformation) new TupleTypeInfo<>(Tuple3.class,
                new TypeInformation[] {keyedInput.getKeyType(), new TupleTypeInfo(Tuple2.class,
                        BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO), foldResultType});

        TypeInformation<Tuple3<K,Tuple4<Double,Double,Long,Long>,RV>> timeResultType = (TypeInformation) new TupleTypeInfo<>(Tuple3.class,
                new TypeInformation[] {keyedInput.getKeyType(), new TupleTypeInfo(Tuple4.class,
                        BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO,BasicTypeInfo.LONG_TYPE_INFO), foldResultType});

        Tuple3<K,Tuple2<Double,Double>, RV> init= new Tuple3<>(null,new Tuple2<>(0d,0d), valueFold.getInit());
        KeyedStream<Tuple3<K,Tuple4<Double,Double,Long,Long>,RV>, Tuple> kPreStream = keyedInput
                .timeWindow(window)
                .apply(init, new LogCountSumFold<>(keySelector,valueSelector,valueFold, resultType),new WindowTimeExtractor(timeResultType))
                .keyBy(0);

        return kPreStream.flatMap(afm);
    }
}