package com.wycweb.bigdata.common.config;

import com.wycweb.bigdata.common.utils.LoadLocalResource;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class BigDataEnvConfig {
    private LoadLocalResource config = new LoadLocalResource();
    private Properties properties = config.getConfig("/env-conf.properties");

    /**
     * 配置spark sql，获得sparksession
     * @return sparksession
     */
    public SparkSession getSparksession() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.sql.broadcastTimeout", "600")
                .set("spark.debug.maxToStringFields", "500")
                .set("hive.metastore.uris", properties.getProperty("hive.metastore.uris"))
                .set("spark.sql.warehouse.dir", properties.getProperty("hive.warehouse.dir"))
                .set("spark.network.timeout", "300s")
                .set("spark.oozie.action.rootlogger.log.level", "DEBUG")
                .set("spark.sql.parquet.binaryAsString", "true")
                .set("hive.exec.dynamic.partition", "true")
                .set("hive.exec.dynamic.partition.mode", "nonstrict")
                .set("spark.task.maxFailures", "5")
                .set("spark.sql.hive.convertMetastoreParquet", "false")
                .set("fs.defaultFS", properties.getProperty("hadoop.fs.defaultFS"));

        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();

        SparkSession.Builder builder = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport();

        SparkSession sparkSession = builder.getOrCreate();
        SparkSession.setActiveSession(sparkSession);
        SparkSession.setDefaultSession(sparkSession);
        return sparkSession;
    }
}
