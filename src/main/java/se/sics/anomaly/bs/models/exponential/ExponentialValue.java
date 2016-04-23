package se.sics.anomaly.bs.models.exponential;

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


import org.apache.flink.api.java.tuple.Tuple2;
import se.sics.anomaly.bs.models.ModelValue;

/**
 * Created by mneumann on 2016-04-21.
 */
public class ExponentialValue extends Tuple2<Double,Double> implements ModelValue {
    public ExponentialValue(){
        super(0d,0d);
    }

    public ExponentialValue(double count, double sum){
        super(count, sum);
    }

    @Override
    public void add(ModelValue v) {
        this.f0 += ((ExponentialValue)v).f0;
        this.f1 += ((ExponentialValue)v).f1;
    }
}

