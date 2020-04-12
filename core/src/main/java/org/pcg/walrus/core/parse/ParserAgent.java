package org.pcg.walrus.core.parse;

import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.LogicalPlan;
import org.pcg.walrus.meta.WMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParserAgent {

    private static final Logger log = LoggerFactory.getLogger(ParserAgent.class);

    /**
     * parse query and return a logical plan<code>LogicalPlan</code>
     */
    public static LogicalPlan parseJob(WMeta meta, String level, long jobId, String type, String logic)
            throws WCoreException {
        BaseParser parser = null;
        switch (type) {
            case CoreConstants.PARSER_TPYE_JSON:
                parser = new WJsonParser();
                break;
            case CoreConstants.PARSER_TPYE_SQL:
                parser = new WSqlParser();
                break;
            default:
                throw new WCoreException("unknown parser type: " + type);
        }
        LogicalPlan plan = parser.parse(meta, logic, level, jobId);
        System.out.println("plan: " + plan);
        log.info("plan: " + plan);
        return plan;
    }
}
