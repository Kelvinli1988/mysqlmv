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

    /**
     * Materialized view status.
     */
    public static enum State {
        /**
         * The MV is created by mysqlmv.create_mv procedure.
         */
        NEW(0),
        /**
         * State for MV setup is done, including delta table created,
         * mapping relationship created, and view expression generated.
         */
        SETUP_FINISH(1),
        /**
         * An error state of MV initialization.
         */
        SETUP_ERROR(-1),
        /**
         * Staus before this is used in JAVA Monitor. The following is used in db procedure.
         *
         * This status indicated the materialized view is ready for initialization load.
         */
        INIT_LOAD_READY(2),
        /**
         * State indicates initialization load error.
         */
        INIT_LOAD_ERROR(-2),
        /**
         * State that init load is finished and ready to updating.
         */
        UPDATE_READY(3),
        /**
         * State that delta is under calculation.
         */
        DELTA_CALCULATING(4),
        /**
         * State that error appears during delta calculation.
         */
        DELTA_CAL_ERROR(-4),
        /**
         * State indicate that delta calculation is done and applying the change to the view.
         */
        UPDATING(5),
        /**
         * Error appears during applying to the view.
         */
        UPDATE_ERROR(-5),
        /**
         * State that indicate the fresh is suspended.
         */
        SUSPENDED(6),

        /**
         * State that indicate the fresh is stopped.
         */
        STOPPED(7);

        private final int stateValue;

        State(int stateValue) {
            this.stateValue = stateValue;
        }

        public int getStateValue() {
            return stateValue;
        }

        public State getState(int value) {
            for(State ss : State.values()) {
                if(ss.getStateValue() == value) {
                    return ss;
                }
            }
            return null;
        }
    }
}
