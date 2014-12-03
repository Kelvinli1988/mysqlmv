package org.mysqlmv.cd.logevent.eventdef.data;

/**
 * Created by Kelvin Li on 12/1/2014 5:33 PM.
 */
public enum RowOperation {
    INSERT(1), UPDATE_I(2), UPDATE_D(3), DELETE(4);
    int value;

    RowOperation(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }
}
