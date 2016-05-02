package se.sics.anomaly.bs.core;

import java.io.Serializable;

/**
 * Created by mneumann on 2016-04-28.
 */
public interface PayloadFold<I,O> extends Serializable {
    O fold(I in, O out);
    O getInit();
}
