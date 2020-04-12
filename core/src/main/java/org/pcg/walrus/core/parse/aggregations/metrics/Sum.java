package org.pcg.walrus.core.parse.aggregations.metrics;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RQuery;

/*
 * sum(m_1) -> just pushdown sum(m_1) to sub query
 */
public class Sum implements MetricFunction {

    @Override
    public String name() {
        return CoreConstants.METRIC_FUNCTION_SUM;
    }

    @Override
    public void apply(ASTNode root, RQuery query, RMetric metric) {
        // do nothing
    }

    @Override
    public String aggregateSql(String params, String alias) {
        return String.format("%s(IF(%s is not null,%s,0)) AS %s", CoreConstants.METRIC_FUNCTION_SUM, alias, alias, alias);
    }

    @Override
    public String selectSql(String method, String params, String alias) {
        return String.format("%s(%s) AS %s", method, params, alias);
    }
}
