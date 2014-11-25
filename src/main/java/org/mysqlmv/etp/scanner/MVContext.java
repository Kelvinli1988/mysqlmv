package org.mysqlmv.etp.scanner;

import org.mysqlmv.etp.mv.MaterializedView;

/**
 * Created by Kelvin Li on 11/24/2014 10:32 AM.
 */
public class MVContext {
    MaterializedView mview;

    public MaterializedView getMview() {
        return mview;
    }

    public void setMview(MaterializedView mview) {
        this.mview = mview;
    }

    public MVContext() {

    }

}
