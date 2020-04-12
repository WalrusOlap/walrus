package org.pcg.walrus.core.plan.operator;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.meta.pb.WFieldMessage;

import java.util.List;

public class Unifier implements Operator {

	private static final long serialVersionUID = 3131847322487434739L;

	private String mode;
	private List<WFieldMessage.WDict> dicts;

	public Unifier(String mode, List<WFieldMessage.WDict> dicts) {
		this.mode = mode;
		this.dicts = dicts;
	}

	public List<WFieldMessage.WDict> getDicts() {
		return dicts;
	}

	public String getMode() {
		return mode;
	}
	
	@Override
	public String getOperatorType() {
		return CoreConstants.AST_OPER_UNION;
	}

	@Override
	public String toString() {
		return "Unifier: " + mode;
	}

}
