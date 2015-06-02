package org.mysqlmv.common.exception;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * This exception is a runtime exception.
 * Created by Kelvin Li on 6/1/2015.
 */
public class CDCException extends Exception {
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The text. */
    private String text;

    /** The id. */
    private int id = -1;

    /** The actual stack trace. */
    private String actualStackTrace;
    /**
     * Exception constructor taking only the text. ID will be empty.
     * @param text Exception text
     */
    public CDCException(String text){
        super(text);
        this.text = text;
    }

    /**
     * Exception constructor taking only the text and fatal flag. ID will be empty.
     * @param text Exception text
     * @param needReconnect tell server if you want to reconnect.
     */
    public CDCException(String text, boolean needReconnect){
        super(text);
        this.text = text;
    }

    /**
     * Exception constructor taking ID and text.
     * @param id Exception ID
     * @param text Exception text
     */
    public CDCException(int id, String text){
        super(text);
        this.id = id;
        this.text = text;
    }

    /**
     * Instantiates a new adapter exception.
     *
     * @param exception the exception
     * @param exceptionMessage the exception message
     */
    public CDCException(Exception exception, String exceptionMessage){
        super(exception);
        this.text = exceptionMessage;
        this.actualStackTrace = CDCException.getExceptionMessage(exception);
    }

    /**
     * Instantiates a new adapter exception.
     *
     * @param exception the exception
     * @since 1.1
     */
    public CDCException(Exception exception){
        super(exception.getMessage());
        this.text = exception.getMessage();
        this.actualStackTrace = CDCException.getExceptionMessage(exception);
    }

    /**
     * Instantiates a new adapter exception.
     *
     * @param throwable the exception
     * @since 1.1
     */
    public CDCException(Throwable throwable){
        super(throwable.getMessage());
        this.text = throwable.getMessage();
        this.actualStackTrace = CDCException.getThrowableMessage(throwable);
    }

    /**
     * Get the exception id.
     *
     * @return Exeption ID
     */
    public int getId() {return id;}

    /**
     * Get the exception text.
     *
     * @return Exception text
     */
    public String getText() {return text;}

    /**
     * Return what the exception is about.
     * The returned string is a special formatting of the two exception components.
     * [ID] Text
     * @return Subject of the exception
     */
    public String what(){
        if(id < 0)
            return text + " Context: " + actualStackTrace;
        else
            return "["+ id +"] "+ text + " Context: " + actualStackTrace;
    }

    /**
     * Prints exception info to the String.
     *
     * @param exception exception to get the message from
     * @return String with detailed information from the exception
     */
    public static String getExceptionMessage ( Exception exception )
    {
        StringWriter sw = new StringWriter() ;    // Output stream into a string.
        PrintWriter out = new PrintWriter(sw) ;   // PrintWriter wrapper.
        exception.printStackTrace(out) ;
        return  sw.getBuffer().toString() ;
    }

    /**
     * Prints throwable info to the String.
     *
     * @param throwable throwable to get the message from
     * @return String with detailed information from the throwable
     * @since 1.1
     */
    public static String getThrowableMessage ( Throwable throwable )
    {
        StringWriter sw = new StringWriter() ;    // Output stream into a string.
        PrintWriter out = new PrintWriter(sw) ;   // PrintWriter wrapper.
        throwable.printStackTrace(out) ;
        return  sw.getBuffer().toString() ;
    }

    /**
     * Prints the first line of the exception info to the String.
     *
     * @return String with top error reported in the exception
     */
    private static final String ADAPTER_SDK_EXCEPTION_TAG = "com.sap.hana.dp.adapter.sdk.AdapterException:";

    /**
     * Gets the exception top message.
     *
     * @param exception the exception
     * @return the exception top message
     */
    public static String getExceptionTopMessage( Exception exception )
    {
        String fullStackMessage = getExceptionMessage(exception);
        while (fullStackMessage.startsWith(ADAPTER_SDK_EXCEPTION_TAG))
            fullStackMessage = fullStackMessage.substring( ADAPTER_SDK_EXCEPTION_TAG.length() ).trim();
        int endOfTopError = fullStackMessage.indexOf("at com.sap.hana.dp.adapter");
        if (endOfTopError < 0)
            return fullStackMessage.trim();
        else
            return fullStackMessage.substring(0,endOfTopError).trim();
    }
}
