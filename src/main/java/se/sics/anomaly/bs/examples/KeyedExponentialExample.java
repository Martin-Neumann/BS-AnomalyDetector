package se.sics.anomaly.bs.examples;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import se.sics.anomaly.bs.core.AnomalyResult;
import se.sics.anomaly.bs.core.PayloadFold;
import se.sics.anomaly.bs.history.History;
import se.sics.anomaly.bs.history.HistoryTrailing;
import se.sics.anomaly.bs.models.exponential.ExponentialValueAnomaly;

import java.util.Random;

public class KeyedExponentialExample {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setParallelism(1);

        // generate stream
        DataStream<Tuple2<String,Double>> inStream = env.addSource(new ExpSource());

        // define history and create model
        History hist = new HistoryTrailing(2);
        ExponentialValueAnomaly<String,Tuple2<String,Double>,NullValue> anomalyDetector = new ExponentialValueAnomaly<String, Tuple2<String, Double>, NullValue>(hist);

        // feed the stream though the model
        DataStream<Tuple3<String,AnomalyResult,NullValue>> result = anomalyDetector.getAnomalySteam(inStream,new KExtract(),"",new VExtract(),new RVFold(),Time.seconds(10));
        // print the result
        result.print();

        env.execute("Simple Exponential Example Keyed");

    }

    private static class KExtract implements KeySelector<Tuple2<String,Double>,String>{
        @Override
        public String getKey(Tuple2<String, Double> t) throws Exception {
            return t.f0;
        }
    }

    private static class VExtract implements KeySelector<Tuple2<String,Double>,Double>{
        @Override
        public Double getKey(Tuple2<String, Double> t) throws Exception {
            return t.f1;
        }
    }

    private static class RVFold implements PayloadFold<Tuple2<String,Double>,NullValue>{
        @Override
        public NullValue fold(Tuple2<String, Double> in, NullValue out) {
            return NullValue.getInstance();
        }

        @Override
        public NullValue getInit() {
            return NullValue.getInstance();
        }
    }

    private static class ExpSource implements SourceFunction<Tuple2<String,Double>> {
        private double lambda1 = 10d;
        private double lambda2 = 100d;
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
                    sourceContext.collect(new Tuple2<>("key2", getNext(lambda1)));
                    if(rnd.nextInt(40)<1)anomaly = true;
                }else{
                    sourceContext.collect(new Tuple2<>("key1", getNext(lambda2)));
                    sourceContext.collect(new Tuple2<>("key2", getNext(lambda2)));
                    if(rnd.nextInt(10)<1)anomaly = false;
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
