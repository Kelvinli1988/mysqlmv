package org.mysqlmv.mvm.mv;

import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * Created by Kelvin Li on 11/21/2014 3:38 PM.
 */
public class MaterializedView {
    private long id;

    private String name;

    private String originalSchema;

    private String defStr;

    private SQLStatement defObj;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOriginalSchema() {
        return originalSchema;
    }

    public void setOriginalSchema(String originalSchema) {
        this.originalSchema = originalSchema;
    }

    public String getDefStr() {
        return defStr;
    }

    public void setDefStr(String defStr) {
        this.defStr = defStr;
    }

    public SQLStatement getDefObj() {
        return defObj;
    }

    public void setDefObj(SQLStatement defObj) {
        this.defObj = defObj;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
