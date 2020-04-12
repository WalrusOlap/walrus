package org.pcg.walrus.core.plan.operator;

import org.apache.commons.lang.ArrayUtils;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.query.RColumn;
import org.pcg.walrus.core.query.RMetric;

import java.util.Date;

/**
 * result Combiner before final aggregation
 */
public class Combiner implements Operator {

    private static final long serialVersionUID = -7806760000857611642L;

    private Date bday;
    private Date eday;

    protected RColumn[] cols;
    protected RMetric[] ms;

    public Combiner(Date bday, Date eday) {
        this.bday = bday;
        this.eday = eday;
    }

    public void setCols(RColumn[] cols) {
        this.cols = cols;
    }

    public void setMs(RMetric[] ms) {
        this.ms = ms;
    }

    public Date getBday() {
        return bday;
    }

    public Date getEday() {
        return eday;
    }

    public RColumn[] getCols() {
        return cols;
    }

    public RMetric[] getMs() {
        return ms;
    }

    @Override
    public String getOperatorType() {
        return CoreConstants.AST_OPER_COMBINER;
    }

    @Override
    public String toString() {
        return "Combiner: {cols: " + ArrayUtils.toString(cols) + "}{MS: " + ArrayUtils.toString(ms) + "}";
    }
}
