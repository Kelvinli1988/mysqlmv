package org.mysqlmv.etp.context;

import java.io.Serializable;

/**
 * Created by Kelvin Li on 12/3/2014 1:33 PM.
 */
public class ToiValue implements Serializable {
    private final int mviewToiId;

    private final int mviewId;

    public int getMviewToiId() {
        return mviewToiId;
    }

    public int getMviewId() {
        return mviewId;
    }

    public ToiValue(int mviewToiId, int mviewId) {
        this.mviewToiId = mviewToiId;
        this.mviewId = mviewId;
    }

    @Override
    public String toString() {
        return "" + mviewToiId + ", " + mviewId;
    }
}
