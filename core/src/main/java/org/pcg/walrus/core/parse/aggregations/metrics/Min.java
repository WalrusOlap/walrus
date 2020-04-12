package org.pcg.walrus.core.parse.aggregations.metrics;

import org.pcg.walrus.core.CoreConstants;

public class Min extends SumSubQuery {

    @Override
    public String name() {
        return CoreConstants.METRIC_FUNCTION_MIN;
    }

    @Override
    public String aggregateSql(String params, String alias) {
        return String.format("%s(%s) AS %s", CoreConstants.METRIC_FUNCTION_MIN, alias, alias);
    }
}
