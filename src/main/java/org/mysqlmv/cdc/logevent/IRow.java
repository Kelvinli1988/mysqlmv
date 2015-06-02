package org.mysqlmv.cdc.logevent;

import java.io.Serializable;
import java.util.Collection;

/**
 * Created by I312762 on 6/1/2015.
 */
public interface IRow extends Serializable {

    /**
     * @return the content of this row.
     */
    Collection<IRowElement> getContent();
}
