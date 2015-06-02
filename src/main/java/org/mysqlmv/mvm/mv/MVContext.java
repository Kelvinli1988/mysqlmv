package org.mysqlmv.mvm.mv;

import org.mysqlmv.mvm.mv.BaseDeltaTable;
import org.mysqlmv.mvm.mv.MaterializedView;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Kelvin Li on 11/24/2014 10:32 AM.
 */
public class MVContext {
    private MaterializedView mview;

    private List<BaseDeltaTable> deltaTableList;

    public MaterializedView getMview() {
        return mview;
    }

    public void setMview(MaterializedView mview) {
        this.mview = mview;
    }

    public MVContext() {
        deltaTableList = new ArrayList<BaseDeltaTable>();
    }

    public List<BaseDeltaTable> getDeltaTableList() {
        return deltaTableList;
    }

    public void setDeltaTableList(List<BaseDeltaTable> deltaTableList) {
        this.deltaTableList = deltaTableList;
    }
}
