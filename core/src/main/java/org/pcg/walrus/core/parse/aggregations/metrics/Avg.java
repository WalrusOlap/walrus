package org.pcg.walrus.core.parse.aggregations.metrics;

import org.pcg.walrus.core.CoreConstants;

/**
 * update sub query function to sum
 */
public class Avg extends SumSubQuery {

    @Override
    public String name() {
        return CoreConstants.METRIC_FUNCTION_AVG;
    }

    // group by appuser first, just count(app_user) as uv
    @Override
    public String aggregateSql(String params, String alias) {
        return String.format("%s(%s) AS %s", CoreConstants.METRIC_FUNCTION_AVG, alias, alias);
    }
}

