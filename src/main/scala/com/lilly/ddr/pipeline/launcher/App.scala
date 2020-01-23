/*******************************************************************************
 * Copyright : (C) Eli Lilly & Company
 * Project: Digital Data Registry
 ******************************************************************************/
package com.lilly.ddr.pipeline.launcher

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import java.util.UUID
import com.lilly.ddr.pipeline.main.EtlPipeline
import org.apache.spark.sql.{ SparkSession, DataFrame }

/**
 * @purpose Local Launcher App
 *
 */
object App {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:/WorkSpaces/Hadoop/hadoop-common-2.2.0-bin-master");
    val sparkSession = SparkSession.builder.master("local[*]").appName(this.getClass.getSimpleName)
      .config("spark.ui.showConsoleProgress", "false").getOrCreate();

    // set error level
    sparkSession.sparkContext.setLogLevel("ERROR")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val glueJobId = UUID.randomUUID().toString
    val propFilePath = args(0)
//    val module = "RawToRefineToConformS3_ETLJob"
//    val module = "createAndPopulateDBTablesJob"
    val module = "RefineToConformRDS_ETLJob"

    var spark_source_delta_df: DataFrame = null
    if (module.equals("RawToRefineToConformS3_ETLJob")) {
      spark_source_delta_df = sparkSession.read.option("mergeSchema", "true").json("D:/Lilly/Lilly-Batch-Job/Raw-Zone/raw_data_streaming/*/*/*/*")
    } else if (module.equals("RefineToConformRDS_ETLJob")) {
      spark_source_delta_df = sparkSession.read.option("mergeSchema", "true").parquet("D:/Lilly/Lilly-Batch-Job/Refine-Zone/")
    }
    
    EtlPipeline.run(sparkSession, glueJobId, propFilePath, module, spark_source_delta_df)
    sparkSession.stop
  }

}


