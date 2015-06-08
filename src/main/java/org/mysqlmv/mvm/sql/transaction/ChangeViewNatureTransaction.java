package org.mysqlmv.mvm.sql.transaction;

import org.mysqlmv.mvm.sql.transaction.exception.BeginTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.CommitTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.RollbackTransactionException;

/**
 * Created by I312762 on 6/8/2015.
 */
public class ChangeViewNatureTransaction implements ITransaction {
    @Override
    public long getTransactionID() {
        return 0;
    }

    @Override
    public void begin() throws BeginTransactionException {

    }

    @Override
    public void commit() throws CommitTransactionException {

    }

    @Override
    public void rollback() throws RollbackTransactionException {

    }
}
