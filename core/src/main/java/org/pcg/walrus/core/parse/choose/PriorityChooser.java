package org.pcg.walrus.core.parse.choose;

import java.util.Collections;
import java.util.List;

import org.pcg.walrus.core.query.RQuery;
import org.pcg.walrus.core.query.RTable;
import org.pcg.walrus.common.exception.WCoreException;

/**
 * PriorityChooser
 * 		choose the <CODE>MTabletNode</CODE> with highest priority for <CODE>RQuery</CODE>
 */
public class PriorityChooser extends ChooserFactory implements IChooser {

	/**
	 * choose table by table PRIORITY
	 */
	@Override
	public RTable apply(List<RTable> cubes, RQuery query) throws WCoreException {
		// sort cubes based on table PRIORITY
		Collections.sort(cubes);
		return choose(cubes, query);
	}
}
