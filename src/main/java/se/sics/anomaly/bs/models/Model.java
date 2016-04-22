package se.sics.anomaly.bs.models;

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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import se.sics.anomaly.bs.core.Anomaly;

import java.io.Serializable;

/**
 * Created by mneumann on 2016-04-21.
 */
public abstract class Model<T extends ModelValue> implements Serializable  {

    public abstract Anomaly calculateAnomaly(T v);
    public abstract void addWindow(T v);
    public abstract TypeInformation getTypeInfo();


}