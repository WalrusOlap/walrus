package org.pcg.walrus.core.parse.choose;

import java.util.List;

import org.pcg.walrus.core.query.RQuery;
import org.pcg.walrus.core.query.RTable;
import org.pcg.walrus.common.exception.WCoreException;

/**
 * choose the most appropriate cube
 */
public interface IChooser {

	public RTable apply(List<RTable> cubes, RQuery query) throws WCoreException;

}
