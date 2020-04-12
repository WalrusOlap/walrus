package org.pcg.walrus.core.parse.choose;

import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.core.query.RQuery;
import org.pcg.walrus.core.query.RTable;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * choose the <CODE>MTabletNode</CODE> with lowest cost for <CODE>RQuery</CODE>
 * define cost:
 *  1 table row number
 *  2 table file number
 */
public class CostChooser extends ChooserFactory implements IChooser {

    @Override
    public RTable apply(List<RTable> cubes, RQuery query) throws WCoreException {
        // sort cubes based on table cost
        Collections.sort(cubes, new Comparator<RTable>() {
            @Override
            public int compare(RTable r1, RTable r2) {
                long row1 = r1.getTableNode().getPartitions().values().iterator().next()
                        .getBaseTableNode().getTabletNode().getTablet().getRecordNum();
//                long file1 = r1.getTableNode().getPartitions().values().iterator().next()
//                        .getBaseTableNode().getTabletNode().getTablet().getFileNum();
                long row2 = r2.getTableNode().getPartitions().values().iterator().next()
                        .getBaseTableNode().getTabletNode().getTablet().getRecordNum();
//                long file2 = r2.getTableNode().getPartitions().values().iterator().next()
//                        .getBaseTableNode().getTabletNode().getTablet().getFileNum();
                return (int) (row2 - row1);
            }
        });
        return choose(cubes, query);
    }
}
