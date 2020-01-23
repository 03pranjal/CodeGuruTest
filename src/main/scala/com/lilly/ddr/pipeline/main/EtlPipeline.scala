/**
 * *****************************************************************************
 * Copyright : (C) Eli Lilly & Company
 * Project: Digital Data Registry
 * ****************************************************************************
 */
package com.lilly.ddr.pipeline.main

import org.apache.spark.sql.{ SparkSession, DataFrame }
import java.util.Properties
import scala.util.Failure
import scala.util.Try
import java.sql.Timestamp
import scala.util.Success
import org.apache.spark.sql.functions.lit
import com.lilly.ddr.pipeline.constant.ConfigValues
import org.postgresql.Driver
import com.lilly.ddr.pipeline.utility.SparkUtility
import com.lilly.ddr.pipeline.utility.EtlUtility
import com.lilly.ddr.pipeline.utility.JdbcUtility
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.{ to_date, to_timestamp }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.{ DriverManager, PreparedStatement, Timestamp, Connection }
import scala.collection.mutable.ListBuffer

/**
 * @purpose This object acts as main entry point of Lilly DDR POCs ETL Pipeline
 */
object EtlPipeline {

   /**
   * Main ETL Job Entry Point
   * @param sparkSession
   * @param glueJobId
   * @param propFilePath
   */
  def run(sparkSession: SparkSession, glueJobId: String, propFilePath: String, module: String, sourceDeltaDf: DataFrame): Unit = {

    val prop = EtlUtility.loadPropertiesFile(propFilePath)

    EtlUtility.getConfigObj(prop) match {
      case Some(config) => {

        EtlUtility.notify("Glue Job Started: " + glueJobId)
        EtlUtility.notify("Jar Version: " + "V22")

        var rawZonePath = config.rawZoneStreamingDataS3Path
        var refineZonePath = config.refineZoneS3Path
        var conformZonePath = config.conformZoneS3Path

        if (scala.util.Properties.envOrNone("s3") == Some("disable")) {
          rawZonePath = config.rawZoneStreamingDataLocalPath
          refineZonePath = config.refineZoneLocalPath
          conformZonePath = config.conformZoneLocalPath
        }

        EtlUtility.notify("rawZonePath: " + rawZonePath)
        EtlUtility.notify("refineZonePath: " + refineZonePath)
        EtlUtility.notify("conformZonePath: " + conformZonePath)
        EtlUtility.notify("DbUrl: " + config.DbUrl)
        EtlUtility.notify("DbUser: " + config.DbUser)

        if (module.equals("RawToRefineToConformS3_ETLJob")) {
          if(sourceDeltaDf != null) {
            processDataFromRawToRefineToConformS3Zone(sparkSession, glueJobId, propFilePath, sourceDeltaDf, refineZonePath, conformZonePath, config)
          } else {
            EtlUtility.notify("Source Data Frame is empty.")
          }
        } else if (module.equals("createAndPopulateDBTablesJob")) {
          JdbcUtility.createAllTables(config)
          JdbcUtility.populateDBDimTables(config)
          JdbcUtility.executeTestQuery(config, "select aggtypid, aggtypename from lilly_ddr_schema.d_aggtype limit 10", "d_aggtype")
          JdbcUtility.executeTestQuery(config, "select subjectid, subject_uniquecd from lilly_ddr_schema.d_subject limit 10", "d_subject")
          JdbcUtility.populateDBDateTimeTable(config)
        } else if (module.equals("RefineToConformRDS_ETLJob")) {
          if(sourceDeltaDf != null) {
          val subjectMap = JdbcUtility.getMapping(config, "select subjectid, subject_uniquecd from lilly_ddr_schema.d_subject order by subjectid;")
          EtlUtility.notify("subjectMap: " + subjectMap)
          val studyMap = JdbcUtility.getMapping(config, "select studyid, studyname from lilly_ddr_schema.d_study order by studyid;")
          EtlUtility.notify("studyMap: " + studyMap)
          val deviceMap = JdbcUtility.getMapping(config, "select deviceid, deviceuniqcd from lilly_ddr_schema.d_device order by deviceid;")
          EtlUtility.notify("deviceMap: " + deviceMap)
          val mobileMap = JdbcUtility.getMapping(config, "select mobileid, mobileuniqcd from lilly_ddr_schema.d_mobile order by mobileid;")
          EtlUtility.notify("mobileMap: " + mobileMap)
          val sensorMap = JdbcUtility.getMapping(config, "select sensorid, sensorname from lilly_ddr_schema.d_sensor order by sensorid;")
          EtlUtility.notify("sensorMap: " + sensorMap)
          val eventMap = JdbcUtility.getMapping(config, "select evtypid, evtypname from lilly_ddr_schema.d_eventtype order by evtypid;")
          EtlUtility.notify("eventMap: " + eventMap)
          val aggMap = JdbcUtility.getMapping(config, "select aggtypid, aggtypename from lilly_ddr_schema.d_aggtype order by aggtypid;")
          EtlUtility.notify("aggMap: " + aggMap)
          val allocationTableMap = SparkUtility.createAllocationTableMap(config, sparkSession)
          EtlUtility.notify("allocationTableMap: " + allocationTableMap)
          
//          val refineS3ZoneDF = sparkSession.read.option("mergeSchema", "true").parquet(refineZonePath)
//          sourceDeltaDf.printSchema()
//          EtlUtility.notify("sourceDeltaDf count: " + sourceDeltaDf.count())
          val refineZoneTempTableName = "refineS3ZoneTable"
          sourceDeltaDf.createOrReplaceTempView(refineZoneTempTableName)

          var distinct_data_type_df = sparkSession.sql("select distinct(data_type_p_col) from " + refineZoneTempTableName)
          var data_type_list = new ListBuffer[String]()
          distinct_data_type_df.collect().foreach { row =>
            data_type_list += row(0).toString()
          }
          EtlUtility.notify("data_type_list: " + data_type_list)
    
          val errorIntentionalRowList = SparkUtility.processIntentionalData(config, sparkSession, refineZoneTempTableName, subjectMap, studyMap, deviceMap, mobileMap, eventMap, allocationTableMap, data_type_list)
          val errorPassiveRowList = SparkUtility.processPassiveData(config, sparkSession, refineZoneTempTableName, subjectMap, studyMap, deviceMap, mobileMap, sensorMap, aggMap, allocationTableMap, data_type_list, refineZonePath)
          val errorRowList = errorIntentionalRowList.prependToList(errorPassiveRowList.toList)
          SparkUtility.populateTempAllocationErrorRecordsTable(sparkSession, config, errorRowList)
          } else {
            EtlUtility.notify("Source Data Frame is empty.")
          }
        }

      }
      case None => EtlUtility.notify("Configuration arguments missing")
    }
  }


   /** 
   * ETL Logic to process data from RAW to Refine to Conform 
   * @param sparkSession
   * @param glueJobId
   * @param propFilePath
   * @param rawZonePath
   * @param refineZonePath
   * @param conformZonePath
   * @param config
   */
  def processDataFromRawToRefineToConformS3Zone(sparkSession: SparkSession, glueJobId: String, propFilePath: String, rawDataDF: DataFrame, refineZonePath: String, conformZonePath: String, config: ConfigValues): Unit = {

    EtlUtility.notify("Executing function processDataFromRawToRefineToConformS3Zone() - Start")

    // Read All Data under RAW S3 ZONE and create Spark SQL Dataframe - FULL LOAD
//    val rawDataDF = sparkSession.read.json(rawZonePath)

//    EtlUtility.notify("RAW Zone Files Schema:-")
//    rawDataDF.printSchema()
//    EtlUtility.notify("Raw Zone Files Record Count: " + rawDataDF.count())
//    EtlUtility.notify("RAW Zone Files Sample Records:-")
//    rawDataDF.show(10)

    // Create Temp table of Raw Zone Files
    rawDataDF.createOrReplaceTempView("raw_data")

    var distinct_data_type_df = sparkSession.sql("select distinct(data_type) from raw_data")
    var data_type_list = new ListBuffer[String]()
    distinct_data_type_df.collect().foreach{row =>
      data_type_list += row(0).toString()
    }
    EtlUtility.notify("data_type_list: " + data_type_list)

    if (data_type_list.contains(config.data_type_accelerometer)) {
    EtlUtility.notify(config.data_type_accelerometer + " records available!")
    // #### Prepare accelerometer Data Set - Start ####
    var accelerometer_outputDF = sparkSession.sql("select * from raw_data where data_type = '" + config.data_type_accelerometer + "'")
    // Drop irrelevant columns
    accelerometer_outputDF = accelerometer_outputDF.drop("x_gyro")
    accelerometer_outputDF = accelerometer_outputDF.drop("y_gyro")
    accelerometer_outputDF = accelerometer_outputDF.drop("z_gyro")
    accelerometer_outputDF = accelerometer_outputDF.drop("heart_beat_count")
    accelerometer_outputDF = accelerometer_outputDF.drop("start_time")
    accelerometer_outputDF = accelerometer_outputDF.drop("stop_time")
    accelerometer_outputDF = accelerometer_outputDF.drop("severity_value")    
    // Convert Required Column from String Type to Timestamp Type 
    accelerometer_outputDF = convertStringToTimestampCol(accelerometer_outputDF, config, config.measurement_time)
    accelerometer_outputDF = addCustomDateColsForAllZone(accelerometer_outputDF, config, config.measurement_time)
    accelerometer_outputDF = addPartitionColsForRefineZone(accelerometer_outputDF, config)
    writeDataIntoRefineZone(sparkSession, refineZonePath, accelerometer_outputDF, config)
    accelerometer_outputDF = addPartitionColsForConformZone(accelerometer_outputDF, config)
    writeDataIntoConformS3Zone(sparkSession, conformZonePath, accelerometer_outputDF, config)
    // #### Prepare accelerometer Data Set - End ####
    }
    
    if (data_type_list.contains(config.data_type_gyro)) {
    EtlUtility.notify(config.data_type_gyro + " records available!")
    // #### Prepare gyro Data Set - Start ####
    var gyro_outputDF = sparkSession.sql("select * from raw_data where data_type = '" + config.data_type_gyro + "'")
    // Drop irrelevant columns
    gyro_outputDF = gyro_outputDF.drop("x_acceleration")
    gyro_outputDF = gyro_outputDF.drop("y_acceleration")
    gyro_outputDF = gyro_outputDF.drop("z_acceleration")
    gyro_outputDF = gyro_outputDF.drop("heart_beat_count")
    gyro_outputDF = gyro_outputDF.drop("start_time")
    gyro_outputDF = gyro_outputDF.drop("stop_time")
    gyro_outputDF = gyro_outputDF.drop("severity_value")
    // Convert Required Column from String Type to Timestamp Type 
    gyro_outputDF = convertStringToTimestampCol(gyro_outputDF, config, config.measurement_time)
    gyro_outputDF = addCustomDateColsForAllZone(gyro_outputDF, config, config.measurement_time)
    gyro_outputDF = addPartitionColsForRefineZone(gyro_outputDF, config)
    writeDataIntoRefineZone(sparkSession, refineZonePath, gyro_outputDF, config)
    gyro_outputDF = addPartitionColsForConformZone(gyro_outputDF, config)
    writeDataIntoConformS3Zone(sparkSession, conformZonePath, gyro_outputDF, config)
    // #### Prepare gyro Data Set - End ####
    }
    
    if (data_type_list.contains(config.data_type_heart_rate)) {
    EtlUtility.notify(config.data_type_heart_rate + " records available!")
    // #### Prepare heart_rate Data Set - Start ####
    var heartRate_outputDF = sparkSession.sql("select * from raw_data where data_type = '" + config.data_type_heart_rate + "'")
    // Drop irrelevant columns
    heartRate_outputDF = heartRate_outputDF.drop("x_acceleration")
    heartRate_outputDF = heartRate_outputDF.drop("y_acceleration")
    heartRate_outputDF = heartRate_outputDF.drop("z_acceleration")
    heartRate_outputDF = heartRate_outputDF.drop("x_gyro")
    heartRate_outputDF = heartRate_outputDF.drop("y_gyro")
    heartRate_outputDF = heartRate_outputDF.drop("z_gyro")
    heartRate_outputDF = heartRate_outputDF.drop("start_time")
    heartRate_outputDF = heartRate_outputDF.drop("stop_time")
    heartRate_outputDF = heartRate_outputDF.drop("severity_value")
    // Convert Required Column from String Type to Timestamp Type 
    heartRate_outputDF = convertStringToTimestampCol(heartRate_outputDF, config, config.measurement_time)
    heartRate_outputDF = addCustomDateColsForAllZone(heartRate_outputDF, config, config.measurement_time)
    heartRate_outputDF = addPartitionColsForRefineZone(heartRate_outputDF, config)
    writeDataIntoRefineZone(sparkSession, refineZonePath, heartRate_outputDF, config)
    heartRate_outputDF = addPartitionColsForConformZone(heartRate_outputDF, config)
    writeDataIntoConformS3Zone(sparkSession, conformZonePath, heartRate_outputDF, config)
    // #### Prepare heart_rate Data Set - End ####
    }

    if (data_type_list.contains(config.data_type_migraine)) {
    EtlUtility.notify(config.data_type_migraine + " records available!")
    // #### Prepare migrane Data Set - Start ####
    var migrane_outputDF = sparkSession.sql("select * from raw_data where data_type = '" + config.data_type_migraine + "'")
    // Drop irrelevant columns
    migrane_outputDF = migrane_outputDF.drop("x_acceleration")
    migrane_outputDF = migrane_outputDF.drop("y_acceleration")
    migrane_outputDF = migrane_outputDF.drop("z_acceleration")
    migrane_outputDF = migrane_outputDF.drop("x_gyro")
    migrane_outputDF = migrane_outputDF.drop("y_gyro")
    migrane_outputDF = migrane_outputDF.drop("z_gyro")
    migrane_outputDF = migrane_outputDF.drop(config.measurement_time)
    migrane_outputDF = migrane_outputDF.drop("heart_beat_count")
    // Convert Required Column from String Type to Timestamp Type 
    migrane_outputDF = convertStringToTimestampCol(migrane_outputDF, config, config.start_time)
    migrane_outputDF = convertStringToTimestampCol(migrane_outputDF, config, config.stop_time)
    migrane_outputDF = addCustomDateColsForAllZone(migrane_outputDF, config, config.start_time)
    migrane_outputDF = addPartitionColsForRefineZone(migrane_outputDF, config)
    writeDataIntoRefineZone(sparkSession, refineZonePath, migrane_outputDF, config)
    migrane_outputDF = addPartitionColsForConformZone(migrane_outputDF, config)
    writeDataIntoConformS3Zone(sparkSession, conformZonePath, migrane_outputDF, config)
    // #### Prepare migrane Data Set - End ####
    }
    
    if (data_type_list.contains(config.data_type_meal)) {
    EtlUtility.notify(config.data_type_meal + " records available!")
    // #### Prepare meal Data Set - Start ####
    var meal_outputDF = sparkSession.sql("select * from raw_data where data_type = '" + config.data_type_meal + "'")
    // Drop irrelevant columns
    meal_outputDF = meal_outputDF.drop("x_acceleration")
    meal_outputDF = meal_outputDF.drop("y_acceleration")
    meal_outputDF = meal_outputDF.drop("z_acceleration")
    meal_outputDF = meal_outputDF.drop("x_gyro")
    meal_outputDF = meal_outputDF.drop("y_gyro")
    meal_outputDF = meal_outputDF.drop("z_gyro")
    meal_outputDF = meal_outputDF.drop(config.measurement_time)
    meal_outputDF = meal_outputDF.drop("heart_beat_count")
    meal_outputDF = meal_outputDF.drop("severity_value")
    // Convert Required Column from String Type to Timestamp Type 
    meal_outputDF = convertStringToTimestampCol(meal_outputDF, config, config.start_time)
    meal_outputDF = convertStringToTimestampCol(meal_outputDF, config, config.stop_time)
    meal_outputDF = addCustomDateColsForAllZone(meal_outputDF, config, config.start_time)
    meal_outputDF = addPartitionColsForRefineZone(meal_outputDF, config)
    writeDataIntoRefineZone(sparkSession, refineZonePath, meal_outputDF, config)
    meal_outputDF = addPartitionColsForConformZone(meal_outputDF, config)
    writeDataIntoConformS3Zone(sparkSession, conformZonePath, meal_outputDF, config)
    // #### Prepare meal Data Set - End ####
    }
    
    // Reading Data From Refine Zone for Testing Purpose Only
//    readDataFromZone(sparkSession, refineZonePath)
    
    // Reading Data From Conform S3 Zone for Testing Purpose Only
//    readDataFromZone(sparkSession, conformZonePath)
    
    EtlUtility.notify("Executing function processDataFromRawToRefineToConformS3Zone() - Completed")
  }

   /**
   * This function is used to Convert Required Column from String Type to Timestamp Type
   * @param outputDf
   * @param config
   * @param timestampColName
   * @return DataFrame
   */
  def convertStringToTimestampCol(outputDf: DataFrame, config: ConfigValues, timestampColName: String): DataFrame = {
    var updatedDF = outputDf
    updatedDF = updatedDF.withColumn(timestampColName,
      to_timestamp(unix_timestamp(updatedDF(timestampColName), config.supportedTimeStampFormatInJsonFile).cast(TimestampType)))
    updatedDF
  }
  
   /**
   * This function is uded to Add Custom Date Columns (Year, Month, Day, Hour)
   * @param outputDf
   * @param config
   * @param timestampColName
   * @return
   */
  def addCustomDateColsForAllZone(outputDf: DataFrame, config: ConfigValues, timestampColName: String): DataFrame = {
    var customDateColAddedDF = outputDf
    customDateColAddedDF = customDateColAddedDF.withColumn(config.year_col, year(col(timestampColName)))
    customDateColAddedDF = customDateColAddedDF.withColumn(config.month_col, month(col(timestampColName)))
    customDateColAddedDF = customDateColAddedDF.withColumn(config.day_col, dayofmonth(col(timestampColName)))
    customDateColAddedDF = customDateColAddedDF.withColumn(config.hour_col, hour(col(timestampColName)))
    customDateColAddedDF
  }
  
   /**
   * This function is used to Add Partition Columns for Refine Zone
   * @param outputDf
   * @param config
   * @return
   */
  def addPartitionColsForRefineZone(outputDf: DataFrame, config: ConfigValues): DataFrame = {
    var partitionedDf = outputDf
    partitionedDf = partitionedDf.withColumn(config.data_type_p_col, col(config.data_type))
    partitionedDf = partitionedDf.withColumn(config.year_p_col, col(config.year_col))
    partitionedDf = partitionedDf.withColumn(config.month_p_col, col(config.month_col))
    partitionedDf = partitionedDf.withColumn(config.day_p_col, col(config.day_col))
    partitionedDf = partitionedDf.withColumn(config.hour_p_col, col(config.hour_col))
    partitionedDf
  }
  
   /**
   * This function is used to Add Partition Columns for Conform Zone
   * @param outputDf
   * @param config
   * @return
   */
  def addPartitionColsForConformZone(outputDf: DataFrame, config: ConfigValues): DataFrame = {
    var partitionedDf = outputDf
    partitionedDf = partitionedDf.withColumn(config.study_type_p_col, col(config.study_type))
    partitionedDf = partitionedDf.withColumn(config.subject_id_p_col, col(config.subject_id))
    partitionedDf = partitionedDf.drop(config.hour_p_col)
    partitionedDf
  }
  
   /**
   * This function is used to Populate Refine Zone
   * @param sparkSession
   * @param refineZonePath
   * @param outputDf
   * @param config
   */
  def writeDataIntoRefineZone(sparkSession: SparkSession, refineZonePath: String, outputDf: DataFrame, config: ConfigValues): Unit = {
    EtlUtility.notify("Writing data into Refine Zone starts:")
//    outputDf.printSchema()
//    outputDf.show()
    outputDf.write.mode(SaveMode.Append).partitionBy(config.data_type_p_col, config.year_p_col, config.month_p_col, config.day_p_col, config.hour_p_col).parquet(refineZonePath)
    EtlUtility.notify("Writing data Completed!")
  }
  
   /**
   * This function is used to Populate Conform Zone
   * @param sparkSession
   * @param conformZonePath
   * @param outputDf
   * @param config
   */
  def writeDataIntoConformS3Zone(sparkSession: SparkSession, conformZonePath: String, outputDf: DataFrame, config: ConfigValues): Unit = {
    EtlUtility.notify("Writing data into Conform Zone starts:")
//    outputDf.printSchema()
//    outputDf.show()
    outputDf.coalesce(1).write.mode(SaveMode.Append).partitionBy(config.study_type_p_col, config.subject_id_p_col, config.data_type_p_col, config.year_p_col, config.month_p_col, config.day_p_col).parquet(conformZonePath)
    EtlUtility.notify("Writing data Completed!")
  }
      
   /**
   * This function is used to Read Data From S3 Zone
   * @param sparkSession
   * @param zonePath
   */
  def readDataFromZone(sparkSession: SparkSession, zonePath: String): Unit = {
    EtlUtility.notify("Reading Zone: " + zonePath)
    var df = sparkSession.read.option("mergeSchema", "true").parquet(zonePath)
    EtlUtility.notify("Schema: ")
    df.printSchema()
    EtlUtility.notify("Records count: " + df.count())
    df.show()
  }
  
}