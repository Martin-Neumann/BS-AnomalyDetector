package se.sics.anomaly.bs;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.flink.streaming.api.windowing.time.Time;
import se.sics.anomaly.bs.gaussian.GaussianFM;

import java.util.Random;

/**
 * Created by mneumann on 2016-04-13.
 */
public class SimpleGaussianExample {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // generate stream
        DataStream<Tuple2<String,Tuple2<Double,Double>>> inStream = env.addSource(new GaussSource());

        // key by identifier and process the window
        KeyedStream<Tuple2<String, Tuple4<Double, Double,Long,Long>>, Tuple> kStream = inStream
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .apply(new PreProcessReduce(),new WindowTimeExtractor())
                .keyBy(0);

        DataStream<Tuple6<String,Double,Double,Long,Long,Double>> modelOut = kStream.flatMap(new GaussianFM(14,10,100));

        modelOut.print();

        /*
        KeyedStream<Tuple2<String, Tuple4<Double, Double, Long, Long>>, Tuple> aggStream =
                inStream.assignTimestamps(new TupleTimeExtractor()).map(new PreProcessTuple()).keyBy(0).timeWindow(Time.of(windowZ, TimeUnit.MILLISECONDS)).apply(new PreProcessReduce(), new WindowTimeExtractor()).keyBy(0);

        aggStream.flatMap(new PoissonFMRolling(cutOff, numSeg,shiftP,shiftN,updateIfAnomaly)).writeAsCsv(oPath).setParallelism(1);
*/
        env.execute("something");

    }

    private static class GaussSource implements SourceFunction<Tuple2<String,Tuple2<Double,Double>>> {
        private volatile NormalDistribution nDist = new NormalDistribution(10,2);
        private volatile NormalDistribution nGarb = new NormalDistribution(100,2);
        private volatile Random rnd = new Random();
        private volatile boolean isRunning = true;



        @Override
        public void run(SourceContext<Tuple2<String, Tuple2<Double, Double>>> sourceContext) throws Exception {
            while(isRunning){
                Thread.sleep(1000);
                if(rnd.nextInt(30)<29){
                    sourceContext.collect(new Tuple2<>("key1", new Tuple2<>(nDist.sample(),1d)));
                    sourceContext.collect(new Tuple2<>("key2",new Tuple2<>(nDist.sample(),1d)));
                }else{
                    sourceContext.collect(new Tuple2<>("key1", new Tuple2<>(nGarb.sample(),1d)));
                    sourceContext.collect(new Tuple2<>("key2",new Tuple2<>(nGarb.sample(),1d)));
                }

            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }


}
