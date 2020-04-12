package org.pcg.walrus.core.parse.aggregations.metrics;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.OperatorVisitor;
import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.plan.operator.Combiner;
import org.pcg.walrus.core.plan.operator.Selector;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RQuery;

import java.util.regex.Pattern;

/**
 * for math expression functions, ! BE CAREFUL OF THIS FUNCTION:
 *  1 only +,-,*,\ is supported,
 *
 * e.g. {"name": "ctr", "method": "math_expression", "params": "click/imp"}
 * e.g. {"name": "ecpm", "method": "math_expression", "params": "income*1000/imp"}
 */
public class MathExpression implements MetricFunction {

    private static String SPLIT_PATTERN = "\\+|-|\\*|/";

    @Override
    public void apply(ASTNode root, RQuery query, RMetric metric) {
        // remove extension metric
        query.removeMetric(metric.getName());
        // get metrics
        String params = metric.getParam().replaceAll("\\(", "").replaceAll("\\)","");
        for(String str: params.split(SPLIT_PATTERN)) {
            String param = extractMetric(str);
            if(param == null) continue;
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
    }

    @Override
    public String name() {
        return CoreConstants.METRIC_FUNCTION_MATH_EXP;
    }

    /**
     * imp/click -> sum(imp)/sum(click)
     */
    @Override
    public String aggregateSql(String params, String alias) {
        String expr = params.replaceAll("\\(", "").replaceAll("\\)","");
        for(String str: params.split(SPLIT_PATTERN)) {
            String param = extractMetric(str);
            if(param != null)
                expr = expr.replace(param, String.format("%s(%s)", CoreConstants.METRIC_FUNCTION_SUM, param));
        }
        return String.format("%s AS %s", expr, alias);
    }

    @Override
    public String selectSql(String method, String params, String alias) {
        return String.format("%s(%s) AS %s", method, params, alias);
    }

    /**
     * if param is a number, is not metric, otherwise is
     */
    private String extractMetric(String param) {
        String p = param.trim();
        try {
            Double.parseDouble(p);
        } catch (NumberFormatException e) {
            return p;
        }
        return null;
    }
}
