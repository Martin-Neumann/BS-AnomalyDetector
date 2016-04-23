package se.sics.anomaly.bs;

import se.sics.anomaly.bs.history.HistoryTrailing;
import se.sics.anomaly.bs.models.exponential.ExponentialValue;

/**
 * Created by mneumann on 2016-04-23.
 */
public class Bla {

    public static void main(String[] args) {
        HistoryTrailing<ExponentialValue> ht = new HistoryTrailing<>(5);

        ExponentialValue ev;
        ev = ht.getHistory();
        System.out.println(ev);
        ht.addWindow(new ExponentialValue(1d,1d));
        ev = ht.getHistory();
        System.out.println(ev);
        ht.addWindow(new ExponentialValue(1d,1d));
        ev = ht.getHistory();
        System.out.println(ev);
        ht.addWindow(new ExponentialValue(1d,1d));
        ev = ht.getHistory();
        System.out.println(ev);
    }
}
