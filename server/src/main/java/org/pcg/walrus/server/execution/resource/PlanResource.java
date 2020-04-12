package org.pcg.walrus.server.execution.resource;

import org.pcg.walrus.common.util.TimeUtil;
import org.pcg.walrus.core.plan.LogicalPlan;
import org.pcg.walrus.meta.MetaConstants;

/**
 * parse from job plan
 */
public class PlanResource implements WResource {

    private static final int TIME_RANGE_ONE = 1;
    private static final int MAX_COLUMNS = 5;

    private LogicalPlan plan;

    private int memoryUnit; // default 2g
    private int executorUnit; // executor unit
    private int maxExecutor; // max executors
    private int minExecutor; // min executors

    public PlanResource(LogicalPlan plan, int memoryUnit,
                        int executorUnit, int maxExecutor, int minExecutor) {
        this.plan = plan;
        this.memoryUnit = memoryUnit;
        this.executorUnit = executorUnit;
        this.maxExecutor = maxExecutor;
        this.minExecutor = minExecutor;
    }

    /**
     * spark executor num: <br>
     *  = EXECUTOR_UNIT * timeRange * tableWeight * dictWeight (<= MAX_EXECUTOR_NUM)
     */
    public int executorNum() {
        double num = executorUnit * timeRange() * tableWeight() * dictWeight();
        num = num > maxExecutor ? maxExecutor : num;
        num = num < minExecutor ? minExecutor : num;
        return (int) num;
    }

    @Override
    public int coreNum() {
        return 1;
    }

    /**
     * spark driver memory
     */
    public String driverMem() {
        return (memoryUnit * 2) + "g";
    }

    /**
     * spark executor memory
     * @return 3~5g
     */
    public String executorMem() {
        double num = planWeight();
        return Math.round(6 * (1.0 / (1 + 1/Math.pow(1.016, num)))) + "g";
    }

    /**
     * time range: <br>
     *	partition H: timeRange = hours / 12 + 1 <br>
     *	partition D: timeRange = days <br>
     *	partition M|Y|A: timeRange = days / 30 + 1 <br>
     */
    protected int timeRange() {
        int range = TIME_RANGE_ONE;
        int diffDay = TimeUtil.diffDays(plan.getBday(), plan.getEday());
        int diffHours = TimeUtil.diffHours(plan.getBday(), plan.getEday());
        switch (plan.partitionMode()) {
            case MetaConstants.PARTITION_MODE_H:
                range = diffHours / 24 + 1;
                break;
            case MetaConstants.PARTITION_MODE_D:
                range = diffDay;
                break;
            case MetaConstants.PARTITION_MODE_M:
                range = diffDay / 10 + 1;
                break;
            case MetaConstants.PARTITION_MODE_Y:
            case MetaConstants.PARTITION_MODE_A:
                range = diffDay / 50 + 1;
                break;
            default:
                break;
        }
        return range;
    }

    // timeRange + 4 * dictSize + tableSize + columns
    // if you want to change this function, be my guest
    private double planWeight() {
        int time = timeRange();
        double dictSize = plan.getDicts().size();
        int tableSize = plan.getTables().size();
        int columnSize = plan.getColumns().size();
        double columns = columnSize <= MAX_COLUMNS ? 1 : Math.pow((columnSize) / 2, 2);
        return time + 4 * dictSize + tableSize + columns;
    }

    // table weight
    private double tableWeight() {
        return 0.5 + sigmoid(plan.getTables().size());
    }

    // dict weight
    // TODO dict should consider partition mode
    private double dictWeight() {
        return 0.5 + sigmoid(plan.getDicts().size());
    }

    // Logistic function
    private double sigmoid(int src) {
        return (1.0 / (1 + Math.exp(-src)));
    }

    @Override
    public String toString() {
        return String.format("--driver-memory %s --num-executors %s --executor-memory %s --executor-cores %s",
                driverMem(), executorNum(), executorMem(), coreNum());
    }
}
