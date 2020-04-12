package org.pcg.walrus.core.parse.aggregations.metrics;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RQuery;

public class CollectList implements MetricFunction {

    @Override
    public String name() {
        return CoreConstants.METRIC_FUNCTION_COLLECT;
    }

    @Override
    public void apply(ASTNode root, RQuery query, RMetric metric) {
        // do nothing
    }

    // spark sql
    @Override
    public String aggregateSql(String params, String alias) {
        return String.format("concat_ws(';', COLLECT_LIST(%s)) AS %s", CoreConstants.METRIC_FUNCTION_COUNT, params, alias);
    }

    @Override
    public String selectSql(String method, String params, String alias) {
        return String.format("concat_ws(';', COLLECT_LIST(%s)) AS %s", CoreConstants.METRIC_FUNCTION_COUNT, params, alias);
    }
}