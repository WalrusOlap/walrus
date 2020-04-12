package org.pcg.walrus.core.plan.optimize;

import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.core.plan.OperatorVisitor;
import org.pcg.walrus.core.plan.node.ASTree;
import org.pcg.walrus.core.plan.operator.Combiner;
import org.pcg.walrus.core.plan.operator.Selector;

/**
 * TODO
 */
public class SkewGroupOptimizer extends OperatorVisitor implements IOpitimizer  {

    @Override
    public void visitSelector(Selector selector) {

    }

    @Override
    public void visitCombiner(Combiner combiner) {

    }

    @Override
    public void optimize(WContext context, ASTree tree) {

    }

    /**
     * if need apply skewJoinOptimizer, run task with parameter "--conf spark.driver.extraJavaOptions=-Dwalrus.optimizer.skew.group=true"
     */
    @Override
    public boolean apply() {
        String skew = System.getProperty("walrus.optimizer.skew.group", "false");
        return "true".equalsIgnoreCase(skew);
    }
    @Override
    public String name() {
        return "data_skew_group_optimizer";
    }
}