package org.mysqlmv.common.util;

import java.util.Collection;

/**
 * Created by Kelvin Li on 12/5/2014 11:12 AM.
 */
public class CollectionUtils {
    public static boolean isEmpty(Collection col) {
        return col == null || col.size() == 0;
    }
}
