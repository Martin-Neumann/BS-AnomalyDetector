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

import se.sics.anomaly.bs.models.ModelValue;
import java.util.ArrayList;

public class HistoryRolling<T extends ModelValue> extends History {


    private ArrayList<T> rollingHistory;

    private int numSegment;
    private int shiftPos;
    private int shiftNeg;
    private int currPos;

    public HistoryRolling(int numSegment, int shiftPos, int shiftNeg){
        this.numSegment = numSegment;
        this.rollingHistory = new ArrayList<>(numSegment);
        this.shiftNeg = shiftNeg;
        this.shiftPos = shiftPos;
        this.currPos = 0;
        for( int i = 0; i<numSegment; i++){
            rollingHistory.add(null);
        }
    }

    @Override
    public T getHistory(){
        boolean notReady = false;

        T sumValue = rollingHistory.get(wrapIndex(currPos + shiftPos));
        if (sumValue == null){
            notReady = true;
        }else{
            for (int i = currPos - shiftNeg; i < currPos + shiftPos; i++){
                T val = rollingHistory.get(wrapIndex(i));
                if (val == null){
                    notReady = true;
                    break;
                }
                sumValue.add(val);
            }
        }

        currPos = wrapIndex(currPos+1);

        if(notReady) return null;
        return sumValue;
    }

    @Override
    public void addWindow(ModelValue v){
        int pos = wrapIndex(currPos-1);
        rollingHistory.set(pos,(T)v);
    }

    private int wrapIndex(int i) {
        int res = i % numSegment;
        if (res < 0) { // java modulus can be negative
            res += numSegment;
        }
        return res;
    }
}
