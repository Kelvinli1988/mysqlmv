package org.mysqlmv.common.io;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.BitSet;

/**
 * Created by Kelvin Li on 11/21/2014 11:52 AM.
 */
public class BitSetTest {
    @Test
    public void parse() throws IOException {
        int inital = 240;
        byte[] input =  new byte[]{(byte) inital};
        BitSet bs = new ByteArrayInputStream(input).readBitSet(5, true);
        Assert.assertEquals(1, 1);
    }
}
