package se.sics.anomaly.bs.history;

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

import org.junit.Test;
import se.sics.anomaly.bs.models.exponential.ExponentialValue;

import static org.junit.Assert.*;

/**
 * Created by mneumann on 2016-04-22.
 */
public class HistoryTrailingTest {

    @Test
    public void testGetHistory() throws Exception {
        HistoryTrailing<ExponentialValue> ht = new HistoryTrailing<>(3);
        ExponentialValue ev;

        ev = ht.getHistory();
        assertNull(ev);

        ht.addWindow(new ExponentialValue(1d,1d));
        ev = ht.getHistory();
        assertNull(ev);

        ht.addWindow(new ExponentialValue(1d,1d));
        ev = ht.getHistory();
        assertNull(ev);

        ht.addWindow(new ExponentialValue(1d,1d));
        ev = ht.getHistory();
        assertEquals(4,ev.f0,0);
        assertEquals(4,ev.f1,0);
    }
}