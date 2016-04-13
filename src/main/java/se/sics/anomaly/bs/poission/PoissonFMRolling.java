package se.sics.anomaly.bs.poission;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by martin on 2015-11-05.
 */
public class PoissonFMRolling extends RichFlatMapFunction<Tuple2<String, Tuple4<Double, Double, Long,Long>>, Tuple9<String,Double,Double,Long,Long,Double,Double,Double,Integer>> {
    private static final Logger logger = LoggerFactory.getLogger(PoissonFMRolling.class);
    private transient ValueState<PoissonMicroModelRolling> microModel;
    private final double threshold;
    private int numSegment;
    private int shiftPos;
    private int shiftNeg;
    private boolean updateIfAnomaly;
    private int muliplicity;

    public PoissonFMRolling(double threshold, int numSegment, int shiftPos, int shiftNeg, int muliplicity, boolean updateIfAnomaly) {
        this.threshold = threshold;
        this.numSegment = numSegment;
        this.shiftNeg = shiftNeg;
        this.shiftPos = shiftPos;
        this.updateIfAnomaly = updateIfAnomaly;
        this.muliplicity = muliplicity;
    }

    public PoissonFMRolling(double threshold, int numSegment, int shiftPos, int shiftNeg, boolean updateIfAnomaly) {
        this.threshold = threshold;
        this.numSegment = numSegment;
        this.shiftNeg = shiftNeg;
        this.shiftPos = shiftPos;
        this.updateIfAnomaly = updateIfAnomaly;
        this.muliplicity = 1;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        PoissonMicroModelRolling init = new PoissonMicroModelRolling(numSegment,shiftPos,shiftNeg);
        ValueStateDescriptor<PoissonMicroModelRolling> descriptor =
                new ValueStateDescriptor<>(
                        "RollingMicroModel",
                        TypeInformation.of(new TypeHint<PoissonMicroModelRolling>() {
                        }),init
                        );
        microModel = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Tuple4<Double, Double, Long, Long>> sample, Collector<Tuple9<String, Double, Double, Long, Long, Double, Double, Double, Integer>> collector) throws Exception {
        PoissonMicroModelRolling model = microModel.value();
        Tuple3<Double,Double,Integer> history = model.getHistory();
        double score = model.calculateAnomaly(sample.f1.f0, sample.f1.f1);

        if ( score <= threshold || updateIfAnomaly){
          model.addWindow(sample.f1.f0, sample.f1.f1);
          microModel.update(model);
        }

        collector.collect(new Tuple9<String, Double, Double, Long, Long, Double,Double,Double,Integer>(sample.f0,sample.f1.f0,sample.f1.f1,sample.f1.f2,sample.f1.f3, score,history.f0,history.f1,history.f2));
    }
}
