package se.sics.anomaly.bs.core;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * Created by mneumann on 2016-04-28.
 */
public interface PayloadFold<I,O> extends Serializable, Function {
    O fold(I in, O out);
    O getInit();
}
