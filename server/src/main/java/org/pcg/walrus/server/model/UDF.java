package org.pcg.walrus.server.model;

import java.util.List;
/**
 * message WUdf
 * {
 * 	optional string className = 1;// 类名
 * 	optional string methodName = 2;// 方法名
 * 	repeated string params = 3;// 参数
 * }
 */
public class UDF {

    private String className;
    private String methodName;
    private List<String> params;

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public List<String> getParams() {
        return params;
    }

    public void setParams(List<String> params) {
        this.params = params;
    }

}
