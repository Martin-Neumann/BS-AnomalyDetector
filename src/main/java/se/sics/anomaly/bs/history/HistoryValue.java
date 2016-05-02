package se.sics.anomaly.bs.history;

/**
 * Created by mneumann on 2016-04-28.
 */


public interface HistoryValue {
    void add(HistoryValue v);
}
