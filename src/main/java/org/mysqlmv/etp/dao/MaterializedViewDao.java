package org.mysqlmv.etp.dao;


import org.mysqlmv.common.util.db.DBUtil;
import org.mysqlmv.common.util.db.QueryCallBack;
import org.mysqlmv.etp.context.ToiEntry;
import org.mysqlmv.etp.mv.MaterializedView;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Kelvin Li on 12/5/2014 10:41 AM.
 */
public class MaterializedViewDao {
    public static List<MaterializedView> findUninitializedMV() {
        return (List<MaterializedView>) DBUtil.executeInPrepareStmt(new QueryCallBack<List<MaterializedView>>() {
            @Override
            public String getSql() {
                return "select * from mview where mview_setup_finished = 0 and mview_name is not null";
            }

            @Override
            public List<MaterializedView> doInCallback(PreparedStatement pstmt) throws SQLException {
                rs = pstmt.executeQuery();
                List<MaterializedView> mvList = new ArrayList<MaterializedView>();
                while (rs.next()) {
                    MaterializedView mv = new MaterializedView();
                    mv.setName(rs.getString("mview_name"));
                    mv.setOriginalSchema(rs.getString("mview_schema"));
                    mv.setDefStr(rs.getString("mview_definition"));
                    mv.setId(rs.getInt("mview_id"));
                    mvList.add(mv);
                }
                return mvList;
            }
        });
    }


    public static List<ToiEntry> findUnsetupToiEntry() {
        return (List<ToiEntry>)DBUtil.executeInPrepareStmt(new QueryCallBack<List<ToiEntry>>() {
            @Override
            public String getSql() {
                return "select distinct schema_name, table_name from mview_toi where setup_finished = 0";
            }
            @Override
            public List<ToiEntry> doInCallback(PreparedStatement pstmt) throws SQLException {
                ResultSet rs = pstmt.getResultSet();
                List<ToiEntry> entryList = new ArrayList<ToiEntry>();
                while(rs.next()) {
                    entryList.add(new ToiEntry(rs.getString("schema_name"),
                            rs.getString("table_name")));
                }
                return entryList;
            }
        });
    }

}
