/*******************************************************************************
 * Copyright : (C) Eli Lilly & Company
 * Project: Digital Data Registry
 ******************************************************************************/

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.{ to_date, to_timestamp }
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.lang.Exception

import com.lilly.ddr.pipeline.main.EtlPipeline

/**
 * @purpose Glue Launcher App
 *
 */
object GlueApp {
  
  /**
   * Main Function
   * @param sysArgs
   */
  def main(sysArgs: Array[String]) {
    
    // Getting Spark Session from Glue Context
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession = glueContext.getSparkSession

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "MODULE").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    // Getting Glue Job Run ID
    val glueJobId = Job.runId.toString

    // Reference property file in Glue Job
    val propFilePath = "config.properties"
    
    val module = args("MODULE")

    // For CDC using Glue DF - Start
    var spark_source_delta_df: DataFrame = null
    if (module.equals("RawToRefineToConformS3_ETLJob")) {
      try {
        val datasource0 = glueContext.getCatalogSource(database = "dh-tti-study-raw-db", tableName = "raw_data_streaming", redshiftTmpDir = "", additionalOptions = JsonOptions("""{"mergeSchema": "true"}"""), transformationContext = "datasource0").getDynamicFrame()
        val recordCount = datasource0.count
        println("Raw Zone Delta record count: " + recordCount)
        println("Raw Zone Schema: ")
        datasource0.printSchema()
        if(recordCount > 0) {
            spark_source_delta_df = datasource0.toDF()
        }
      } catch {
        case x: Exception => 
        {  
            println("Exception Raw Zone Dataframe is Empty") 
        } 
      }
    } else if (module.equals("RefineToConformRDS_ETLJob")) {
      try {
        val datasource0 = glueContext.getCatalogSource(database = "dh-tti-study-refine-db", tableName = "lly_dh_tti_ddr_refine_dev", redshiftTmpDir = "", additionalOptions = JsonOptions("""{"mergeSchema": "true"}"""), transformationContext = "datasource0").getDynamicFrame()
        val recordCount = datasource0.count
        println("Refine Zone Delta record count: " + recordCount)
        println("Refine Zone Schema: ")
        datasource0.printSchema()
        if(recordCount > 0) {
            spark_source_delta_df = datasource0.toDF()
        }
      } catch {
        case x: Exception => 
        {  
            println("Exception Refine Zone Dataframe is Empty") 
        } 
      }
    }
    // For CDC using Glue DF - End    
    
    EtlPipeline.run(sparkSession, glueJobId, propFilePath, module, spark_source_delta_df)
    
    sparkSession.stop
    Job.commit()
  }
}
