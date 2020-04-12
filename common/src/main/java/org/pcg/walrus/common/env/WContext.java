package org.pcg.walrus.common.env;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;

/**
 * runtime context, i.e. spark, pig, kudu, druid... </br>
 */
public class WContext {

    // spark
    private SparkSession sparkSession;
    private JavaSparkContext jsc;

    /**
     * init spark session
     */
    public WContext(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
    }

    public SQLContext getSqlContext() {
        return sparkSession.sqlContext();
    }

    public SparkContext getSc() {
        return sparkSession.sparkContext();
    }

    public SparkSession getSession() {
        return sparkSession;
    }

    public JavaSparkContext getJSc() {
        return jsc;
    }
}

