package org.mysqlmv.mvm.sql.transaction;

import org.mysqlmv.mvm.sql.transaction.exception.BeginTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.CommitTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.RollbackTransactionException;

import java.util.ArrayList;
import java.util.List;

/**
 * Global transaction for materialized view initialization.
 *
 * Created by Kelvin Li on 6/3/2015.
 */
public class MVInitliazeGlobalTransaction implements ITransaction {

    private List<ITransaction> transactionList = new ArrayList<ITransaction>();

    private final long transactionId;

    public MVInitliazeGlobalTransaction(long tranId) {
        this.transactionId = tranId;
    }

    @Override
    public long getTransactionID() {
        return transactionId;
    }

    @Override
    public void begin() throws BeginTransactionException {
        for(ITransaction tran : transactionList) {
            tran.begin();
        }
    }

    @Override
    public void commit() throws CommitTransactionException {
        for(ITransaction tran : transactionList) {
            tran.commit();
        }
    }

    @Override
    public void rollback() throws RollbackTransactionException {
        for(ITransaction tran : transactionList) {
            tran.rollback();
        }
    }

    public void join(ITransaction tran) {
        transactionList.add(tran);
    }
}
