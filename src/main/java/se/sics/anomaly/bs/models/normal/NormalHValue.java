package se.sics.anomaly.bs.models.normal;

import org.apache.flink.api.java.tuple.Tuple4;
import se.sics.anomaly.bs.history.HistoryValue;

/**
 * Created by mneumann on 2016-04-28.
 */
public class NormalHValue extends Tuple4<Double,Double,Double,Double> implements HistoryValue {
    public NormalHValue(){
        super(0d, 0d,0d,0d);
    }

    @Override
    public void add(HistoryValue v) {
        this.f0 +=((NormalHValue)v).f0;
        this.f1 +=((NormalHValue)v).f1;
        this.f2 +=((NormalHValue)v).f2;
        this.f3 +=((NormalHValue)v).f3;
    }

    @Override
    public HistoryValue getEmpty() {
        return new NormalHValue();
    }

}
