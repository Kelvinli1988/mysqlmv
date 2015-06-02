package org.mysqlmv.etp.mv;

import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;

import java.lang.reflect.Method;

/**
 * Created by Kelvin Li on 11/24/2014 1:27 PM.
 */
public class SqlMonitorVisitor extends MySqlOutputVisitor{
    public SqlMonitorVisitor(Appendable appender) {
        super(appender);
    }

    public void addFunction() {
        Class superClazz = SQLASTOutputVisitor.class;
        Method[] mms = superClazz.getDeclaredMethods();
        for(Method method : mms) {
            if(method.getParameterTypes().length == 0) {
                continue;
            }
            System.out.print("public ");
            System.out.print(method.getReturnType().getName() + " ");
            System.out.print(method.getName() + " (");
            for(int i=0; i<method.getParameterTypes().length; i++) {
                String paramType = method.getParameterTypes()[i].getName();
                System.out.print(paramType + " param" + i);
            }
            System.out.println(") {");
            System.out.println("System.out.println(param0.getClass().getName());");
            if(method.getReturnType().getName().equals("void")) {
                System.out.println("super." + method.getName() + "(param0);");
            } else {
                System.out.println("return super." + method.getName() +"(param0);");
            }
            System.out.println("}");
        }
    }
}
