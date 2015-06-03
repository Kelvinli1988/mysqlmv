package org.mysqlmv.mvm.sql.transaction.exception;

/**
 * Created by Kelvin Li on 6/3/2015.
 */
public class CommitTransactionException extends TransactionException {
    public CommitTransactionException(String text) {
        super(text);
    }

    public CommitTransactionException(String text, boolean needReconnect) {
        super(text, needReconnect);
    }

    public CommitTransactionException(int id, String text) {
        super(id, text);
    }

    public CommitTransactionException(Exception exception, String exceptionMessage) {
        super(exception, exceptionMessage);
    }

    public CommitTransactionException(Exception exception) {
        super(exception);
    }

    public CommitTransactionException(Throwable throwable) {
        super(throwable);
    }
}
