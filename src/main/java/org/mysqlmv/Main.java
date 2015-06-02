package org.mysqlmv;

import org.mysqlmv.common.exception.CDCException;
import org.mysqlmv.mvm.MaterializedViewMonitor;
import org.mysqlmv.mvm.mv.MaterializedView;

import java.util.List;

/**
 * Created by I312762 on 6/1/2015.
 */
public class Main {
    public static void main(String[] args) throws CDCException {
        MaterializedViewMonitor mvm = new MaterializedViewMonitor();
        List<MaterializedView> mvList = mvm.findUnintializedMV();
        for(MaterializedView mv : mvList) {
            mvm.initializeMV(mv);
        }
    }
}
