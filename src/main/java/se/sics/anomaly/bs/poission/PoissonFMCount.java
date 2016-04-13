package se.sics.anomaly.bs.poission;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by martin on 2015-11-05.
 */
public class PoissonFMCount extends RichFlatMapFunction<Tuple2<String, Tuple4<Double, Double, Long,Long>>, Tuple6<String,Double,Double,Long,Long,Double>> {
    private static final Logger logger = LoggerFactory.getLogger(PoissonFMCount.class);
    private OperatorState<PoissonMicroModelCount> microModel;
    private final double threshold;
    private final int maxHistory;

    public PoissonFMCount(double threshold, int maxHistory) {
        this.threshold = threshold;
        this.maxHistory = maxHistory;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        PoissonMicroModelCount init = new PoissonMicroModelCount(maxHistory);
        microModel = getRuntimeContext().getKeyValueState("microModel", PoissonMicroModelCount.class, init);
    }

    @Override
    public void flatMap(Tuple2<String, Tuple4<Double, Double, Long, Long>> sample, Collector<Tuple6<String, Double, Double, Long, Long, Double>> collector) throws Exception {
        PoissonMicroModelCount model = microModel.value();
        double score = model.calculateAnomaly(sample.f1.f0, sample.f1.f1);

        if (score > threshold) {
           // logger.info("Found anomaly: score \'" + score + "\' message \'" + sample + "\'");
        }


        //if ( score <= threshold || !model.isHistoryFull()){
            model.addWindow(sample.f1.f0, sample.f1.f1);
            microModel.update(model);
            //logger.info("Model updated: " + model.toString());
        //}

        collector.collect(new Tuple6<String, Double, Double, Long, Long, Double>(sample.f0,sample.f1.f0,sample.f1.f1,sample.f1.f2,sample.f1.f3, score));
    }
}
