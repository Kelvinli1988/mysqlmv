package org.mysqlmv.cdc.logevent.schema;

/**
 * Created by I312762 on 5/22/2015.
 */
public interface ITableName extends IName {

    /**
     * @return the name of the owner, usually database name in mysql.
     */
    public String getOwnerName();

    /**
     * Change the owner of the table.
     * @param owner
     */
    public void setOwnerName(String owner);

    /**
     * Change the name of the table.
     * @param name
     */
    public void changeName(ITableName name);

    /**
     * @return the name of the database the table is in.
     */
    public String getDatabaseName();

    /**
     * @return the fully-qualified article name without quotes.
     */
    public String getQualifiedName();

}
