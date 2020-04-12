package org.pcg.walrus.core.parse.aggregations.metrics;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.plan.operator.Aggregator;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RQuery;

/**
 * query: set function = count
 * aggregator: set function = sum
 */
public class Count implements MetricFunction {

    @Override
    public String name() {
        return CoreConstants.METRIC_FUNCTION_COUNT;
    }

    @Override
    public void apply(ASTNode root, RQuery query, RMetric metric) {
        Aggregator aggregator = (Aggregator) root.getOperator();
        for(RMetric m: aggregator.getMs())
            if(m.getName().equalsIgnoreCase(metric.getName()))
                m.setMethod(CoreConstants.METRIC_FUNCTION_SUM);
    }

    // spark sql
    public String aggregateSql(String params, String alias) {
        return String.format("%s(1) AS %s", CoreConstants.METRIC_FUNCTION_COUNT, alias);
    }

    @Override
    public String selectSql(String method, String params, String alias) {
        return String.format("%s(%s) AS %s", method, params, alias);
    }
}
