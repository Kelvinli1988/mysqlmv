package org.mysqlmv.mvm.sql.transaction.exception;

/**
 * Created by Kelvin Li on 6/3/2015.
 */
public class RollbackTransactionException extends TransactionException {
    public RollbackTransactionException(String text) {
        super(text);
    }

    public RollbackTransactionException(String text, boolean needReconnect) {
        super(text, needReconnect);
    }

    public RollbackTransactionException(int id, String text) {
        super(id, text);
    }

    public RollbackTransactionException(Exception exception, String exceptionMessage) {
        super(exception, exceptionMessage);
    }

    public RollbackTransactionException(Exception exception) {
        super(exception);
    }

    public RollbackTransactionException(Throwable throwable) {
        super(throwable);
    }
}
