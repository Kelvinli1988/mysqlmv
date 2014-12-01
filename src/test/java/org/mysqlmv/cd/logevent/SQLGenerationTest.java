package org.mysqlmv.cd.logevent;

import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

/**
 * Created by Kelvin Li on 11/27/2014 11:13 AM.
 */
public class SQLGenerationTest {
    @Test
    public void generateSQL() throws FileNotFoundException {
        File sqlFile01 = new File("c:/workspace/test/sql03.sql");
        PrintWriter pw = new PrintWriter(sqlFile01);
        for(int i=0;i<1000000;i++) {
            pw.println("insert into test_log_write_trigger_point01(name) values('val"+ i +"');");
        }
        pw.flush();
    }
}