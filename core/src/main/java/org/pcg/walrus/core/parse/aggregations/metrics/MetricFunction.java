package org.pcg.walrus.core.parse.aggregations.metrics;

import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RQuery;

/**
 * rewrite aggregation tree if necessary
 */
public interface MetricFunction {

    public String name(); // function name

    public String selectSql(String method, String params, String alias); // default spark sql

    public String aggregateSql(String params, String alias); // spark sql with method

    public void apply(ASTNode root, RQuery query, RMetric metric);
}
