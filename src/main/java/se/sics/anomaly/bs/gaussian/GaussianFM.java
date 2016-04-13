package se.sics.anomaly.bs.gaussian;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by martin on 2015-11-05.
 */
public class GaussianFM extends RichFlatMapFunction<Tuple2<String, Tuple4<Double, Double, Long,Long>>, Tuple6<String,Double,Double,Long,Long,Double>> {
    private static final Logger logger = LoggerFactory.getLogger(GaussianFM.class);
    private OperatorState<GaussianMicroModel> microModel;
    private final double threshold;
    private final long windowLength;
    private final double maxHistoryLength;

    public GaussianFM(double threshold, long windowLength, double historyDecay) {
        this.threshold = threshold;
        this.windowLength = windowLength;
        this.maxHistoryLength = historyDecay;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        GaussianMicroModel initModel = new GaussianMicroModel(maxHistoryLength);
        this.microModel = getRuntimeContext().getKeyValueState("microModel", TypeExtractor.getForObject(initModel) ,initModel);
    }


    @Override
    public void flatMap(Tuple2<String, Tuple4<Double, Double, Long, Long>> sample, Collector<Tuple6<String, Double, Double, Long, Long, Double>> collector) throws Exception {
        GaussianMicroModel model = microModel.value();
        double value = sample.f1.f1/sample.f1.f0;
        double score = model.calculateAnomaly(value, windowLength);

        /*
        if (score > threshold) {
            collector.collect(new Tuple2<>(sample.f0,score));
        }
        */
        Tuple6<String, Double, Double, Long, Long, Double> res = new Tuple6<>(sample.f0,sample.f1.f0,sample.f1.f1,sample.f1.f2,sample.f1.f3,score);

        if ( score <= threshold || !model.isHistoryFull()){
            // update model
            model.addWindow(value,windowLength);
            microModel.update(model);
        }

        collector.collect(res);
    }
}

