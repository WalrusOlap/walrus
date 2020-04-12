package org.pcg.walrus.core.execution.job.stage;

import com.google.common.collect.Lists;
import org.pcg.walrus.core.execution.job.RStage;
import org.pcg.walrus.core.plan.operator.Operator;
import org.pcg.walrus.core.query.RPartition;
import org.pcg.walrus.common.env.WContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class BaseStage implements RStage {

    protected static final Logger log = LoggerFactory.getLogger(BaseStage.class);

    protected transient List<RStage> stages;
    protected transient WContext context;
    protected transient RPartition partition;
    protected transient Operator operator;

    public BaseStage(WContext context, RPartition partition, Operator operator) {
        this.context = context;
        this.partition = partition;
        this.operator = operator;
        stages = Lists.newArrayList();
    }

    @Override
    public void addStage(RStage stage) {
        stages.add(stage);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(operator.getOperatorType());
        for(RStage child: stages) sb.append("->" + child);
        return sb.toString();
    }
}
