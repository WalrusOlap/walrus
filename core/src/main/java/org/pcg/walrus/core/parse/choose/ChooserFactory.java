package org.pcg.walrus.core.parse.choose;

import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.core.parse.ParseValidator;
import org.pcg.walrus.core.query.RQuery;
import org.pcg.walrus.core.query.RTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class ChooserFactory extends ParseValidator {

    protected static final Logger log = LoggerFactory.getLogger(PriorityChooser.class);

    private static final String STRATEGY_PRIORITY = "priority";
    private static final String STRATEGY_COST = "cost";

    private static Map<String, IChooser> map = new HashMap();

    static {
        map.put(STRATEGY_PRIORITY, new PriorityChooser());
        map.put(STRATEGY_COST, new CostChooser());
    }

    /**
     * load IChooser based on {strategy}
     */
    public static IChooser loadChooser(String strategy) {
        return map.getOrDefault(strategy.toLowerCase(), new PriorityChooser());
    }

    protected RTable choose(List<RTable> cubes, RQuery query) {
        // init candidates
        List<TCandidate> candidates = new ArrayList();
        log.info("choose cubes: " + cubes);
        for(RTable cube: cubes) {
            try {
                validateTable(cube, query, 100001);
                // if validate succeed, return cube
                return cube;
            } catch (WCoreException e) {
                log.error(String.format("choose cube %s error: %s", cube.getTableNode().getName(), e.getMessage()));
                candidates.add(new TCandidate(cube, e.getErrorCode(), e.getErrorMsg()));
            }
        }
        // if all cube validate failed:
        //  1: if all cube failed with same error: return first one(with highest priority)
        //  2: else return last one
        boolean sameError = true;
        TCandidate t = candidates.get(0);
        for(TCandidate c: candidates) sameError = sameError & t.equals(c);
        if(sameError) return candidates.get(0).cube;
        else return candidates.get(candidates.size() - 1).cube;
    }

    /**
     * table candidates
     */
    private class TCandidate {
        public RTable cube;
        public int errorCode;
        public String msg;
        public TCandidate(RTable cube, int errorCode, String msg) {
            this.cube = cube;
            this.errorCode = errorCode;
            this.msg = msg;
        };

        @Override
        public int hashCode() {
            return errorCode + msg.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            ChooserFactory.TCandidate c = (ChooserFactory.TCandidate) obj;
            return errorCode == c.errorCode
                    && msg.equalsIgnoreCase(c.msg);
        }
    }
}
