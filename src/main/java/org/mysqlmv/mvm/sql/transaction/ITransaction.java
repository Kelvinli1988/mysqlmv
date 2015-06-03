package org.mysqlmv.mvm.sql.transaction;

import org.mysqlmv.mvm.sql.transaction.exception.BeginTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.CommitTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.RollbackTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.TransactionException;

/**
 * This interface will minic a tranction, allowing a begin, commit and rollback.
 * Created by Kelvin Li on 6/3/2015.
 */
public interface ITransaction {

    /**
     * Get transaction ID.
     */
    public long getTransactionID();

    /**
     * Begin a dummy transaction.
     */
    public void begin() throws BeginTransactionException;

    /**
     * Commit a dummy transaction.
     */
    public void commit() throws CommitTransactionException;

    /**
     * Roll back a dummy transaction.
     */
    public void rollback() throws RollbackTransactionException;
}
