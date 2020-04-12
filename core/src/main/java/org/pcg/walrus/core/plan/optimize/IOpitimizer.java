package org.pcg.walrus.core.plan.optimize;

import org.pcg.walrus.core.plan.node.ASTree;
import org.pcg.walrus.common.env.WContext;

import java.util.List;

/**
 * logic plan optimizer </br>
 */
public interface IOpitimizer {
	public void optimize(WContext context, ASTree tree);
	public boolean apply();
	public String name();
}