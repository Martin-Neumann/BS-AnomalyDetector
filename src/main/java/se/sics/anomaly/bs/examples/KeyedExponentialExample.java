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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import se.sics.anomaly.bs.core.KeyedAnomalyFlatMap;
import se.sics.anomaly.bs.history.History;
import se.sics.anomaly.bs.history.HistoryTrailing;
import se.sics.anomaly.bs.models.exponential.ExponentialModel;
import se.sics.anomaly.bs.models.exponential.ExponentialValue;

import java.util.Random;

public class KeyedExponentialExample {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setParallelism(1);

        // generate stream
        DataStream<Tuple2<String,Double>> inStream = env.addSource(new ExpSource());

        // key by identifier and pre-process the window
        KeyedStream<Tuple3<String,ExponentialValue,NullValue>, Tuple> kPreStream = inStream
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .fold(new Tuple3<>("",new ExponentialValue(0d,0d), NullValue.getInstance()), new PreProcessFold())
                .keyBy(0);

        // initialize model
        History<ExponentialValue> hist = new HistoryTrailing<ExponentialValue>(5);
        KeyedAnomalyFlatMap<String,ExponentialModel,ExponentialValue,NullValue> afm = new KeyedAnomalyFlatMap<>(14d,new ExponentialModel(hist), true);

        kPreStream.flatMap(afm).print();

        env.execute("Simple Exponential Example");

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

    private static class PreProcessFold implements FoldFunction<Tuple2<String,Double>,Tuple3<String,ExponentialValue, NullValue>> {
        @Override
        public Tuple3<String,ExponentialValue, NullValue> fold(Tuple3<String,ExponentialValue, NullValue> out, Tuple2<String, Double> o) throws Exception {
            out.f1.f0 += 1;
            out.f1.f1 += o.f1;
            if(out.f0.equalsIgnoreCase(""))out.f0 = o.f0;
            return out;
        }
    }


}
