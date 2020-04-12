package org.pcg.walrus.core.execution;

import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.core.plan.LogicalPlan;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.meta.pb.WJobMessage;

public interface IExecutor {

	public WResult execute(WContext context, LogicalPlan plan, WJobMessage.WJob job) throws WCoreException;

}
