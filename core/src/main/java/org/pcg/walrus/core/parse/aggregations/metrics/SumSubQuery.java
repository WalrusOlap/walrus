package org.pcg.walrus.core.parse.aggregations.metrics;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.OperatorVisitor;
import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.plan.operator.Combiner;
import org.pcg.walrus.core.plan.operator.Selector;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RQuery;

/**
 * query:
 *  1 remove metric
 *  2 add metric {param} and set function to sum
 * combiner:
 *  1 remove metric
 *  2 add metric {param} and set function to sum
 */
public class SumSubQuery implements MetricFunction {

    @Override
    public String name() {
        return "SUM_SUB_QUERY";
    }

    @Override
    public void apply(ASTNode root, RQuery query, RMetric metric) {
        String param = null;
        if(metric.getParam() != null) param = metric.getParam(); // param from meta
        else param = metric.getName(); // param from query
        // remove uv
        query.removeMetric(metric.getName());
        // add ${PARAM}
        RMetric m = new RMetric(param);
        m.setMethod(CoreConstants.METRIC_FUNCTION_SUM);
        query.addMetric(m);
        // add sub aggregation stage if necessary
        if(root.childNum() > 0) {
            root.traverseDown(new OperatorVisitor() {
                @Override
                public void visitSelector(Selector selector) {
                    // do nothing
                }
                @Override
                public void visitCombiner(Combiner combiner) {
                    combiner.setMs(RMetric.removeMetric(combiner.getMs(), metric));
                    combiner.setMs(RMetric.addMetric(combiner.getMs(), m));
                }
            });
        }
    }

    @Override
    public String aggregateSql(String params, String alias) {
        return String.format("%s(%s) AS %s", CoreConstants.METRIC_FUNCTION_SUM, alias, alias);
    }

    @Override
    public String selectSql(String method, String params, String alias) {
        return String.format("%s(%s) AS %s", method, params, alias);
    }
}