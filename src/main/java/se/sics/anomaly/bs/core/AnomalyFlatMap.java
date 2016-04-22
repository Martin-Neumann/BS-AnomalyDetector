package se.sics.anomaly.bs.core;

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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import se.sics.anomaly.bs.models.Model;
import se.sics.anomaly.bs.models.ModelValue;


public class AnomalyFlatMap<M extends Model,V extends ModelValue, T> extends RichFlatMapFunction<Tuple2<V, T>, Tuple2<Anomaly,T>> {
    private transient ValueState<M> microModel;
    private final double threshold;
    private boolean updateIfAnomaly;
    private M initModel;

    public AnomalyFlatMap(double threshold, M model, boolean updateIfAnomaly) {
        this.threshold = threshold;
        this.updateIfAnomaly = updateIfAnomaly;
        this.initModel = model;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<M> descriptor =
                new ValueStateDescriptor<>(
                        "RollingMicroModel",
                        initModel.getTypeInfo(),
                        initModel
                        );
        microModel = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<V, T> sample, Collector<Tuple2<Anomaly, T>> collector) throws Exception {
        M model = microModel.value();
        Anomaly res  = model.calculateAnomaly(sample.f0);

        if ( res.getScore() <= threshold || updateIfAnomaly){
            model.addWindow(sample.f0);
            microModel.update(model);
        }
        collector.collect(new Tuple2<>(res,sample.f1));
    }
}
