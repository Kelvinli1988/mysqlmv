package org.mysqlmv.mvm.sql.transaction.exception;

/**
 * Begin transaction exception.
 * Created by Kelvin Li on 6/3/2015.
 */
public class BeginTransactionException extends TransactionException {
    public BeginTransactionException(String text) {
        super(text);
    }

    public BeginTransactionException(String text, boolean needReconnect) {
        super(text, needReconnect);
    }

    public BeginTransactionException(int id, String text) {
        super(id, text);
    }

    public BeginTransactionException(Exception exception, String exceptionMessage) {
        super(exception, exceptionMessage);
    }

    public BeginTransactionException(Exception exception) {
        super(exception);
    }

    public BeginTransactionException(Throwable throwable) {
        super(throwable);
    }
}
