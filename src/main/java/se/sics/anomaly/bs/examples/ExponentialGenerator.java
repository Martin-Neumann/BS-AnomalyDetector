package se.sics.anomaly.bs.examples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * Created by mneumann on 2016-05-09.
 */
public class ExponentialGenerator implements SourceFunction<Tuple2<String,Double>> {

        private double lambda1 = 10d;
        private volatile Random rnd = new Random();
        private volatile boolean isRunning = true;
        private volatile boolean anomaly = false;

        public double getNext(double lambda) {
            return  Math.log(1-rnd.nextDouble())/(-lambda);
        }

        @Override
        public void run(SourceContext<Tuple2<String, Double>> sourceContext) throws Exception {
            while(isRunning){
                Thread.sleep(1000);
                if(!anomaly){
                    sourceContext.collect(new Tuple2<>("key1", getNext(lambda1)));
                    //sourceContext.collect(new Tuple2<>("key2", getNext(lambda1)));
                    if(rnd.nextInt(40)<1)anomaly = true;
                }else{
                    sourceContext.collect(new Tuple2<>("key1", getNext(lambda1)*20));
                    //sourceContext.collect(new Tuple2<>("key2", getNext(lambda1)*20));
                    if(rnd.nextInt(10)<1)anomaly = false;
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

}
