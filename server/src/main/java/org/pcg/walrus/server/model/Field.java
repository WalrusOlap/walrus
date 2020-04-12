package org.pcg.walrus.server.model;

/**
 * org.pcg.walrus.meta.pb.{WDimension|WMetric}
 */
public class Field {

    protected String name;
    protected String type;
    protected String group;
    protected String chinese;

    protected String method; // 派生类型
    protected String derivedMode; // 派生类型

    protected UDF derivedLogic; // 派生字段逻辑

    public Field(String name) {
        setName(name);
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getChinese() {
        return chinese;
    }

    public void setChinese(String chinese) {
        this.chinese = chinese;
    }

    public String getDerivedMode() {
        return derivedMode;
    }

    public void setDerivedMode(String derivedMode) {
        this.derivedMode = derivedMode;
    }

    public UDF getDerivedLogic() {
        return derivedLogic;
    }

    public void setDerivedLogic(UDF derivedLogic) {
        this.derivedLogic = derivedLogic;
    }
}
