package org.pcg.walrus.core.parse.aggregations.metrics;

import org.pcg.walrus.core.CoreConstants;

/**
 * update sub query function to sum
 */
public class Max extends SumSubQuery {

    @Override
    public String name() {
        return CoreConstants.METRIC_FUNCTION_MAX;
    }

    @Override
    public String aggregateSql(String params, String alias) {
        return String.format("%s(%s) AS %s", CoreConstants.METRIC_FUNCTION_MAX, alias, alias);
    }
}
