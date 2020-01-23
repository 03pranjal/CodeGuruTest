/**
 * *****************************************************************************
 * Copyright : (C) Eli Lilly & Company
 * Project: Digital Data Registry
 * ****************************************************************************
 */
package com.lilly.ddr.pipeline.utility

import org.apache.spark.sql.{ SparkSession, DataFrame, SaveMode }
import org.apache.spark.sql.types.{ StructType, StructField }
import org.apache.spark.sql.functions.{ lit, trim }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import scala.collection.immutable._
import scala.collection.mutable.ListBuffer
import com.lilly.ddr.pipeline.constant.ConfigValues
import java.lang.Double
import java.util.Date
import java.text.SimpleDateFormat
import java.sql.Timestamp


/**
 * It acts as a utility object required for Spark Operations
 *
 */
object SparkUtility {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  
  def getTrimmedDataframe(rawOutputDataDf: DataFrame): DataFrame = {
    var OutputDataDf = rawOutputDataDf
    val fileSchema = OutputDataDf.schema
    fileSchema.foreach { fileStructField =>
      if (fileStructField.dataType.typeName.toLowerCase().equals("string")) {
        OutputDataDf = OutputDataDf.withColumn(fileStructField.name, trim(OutputDataDf(fileStructField.name)))
      }
    }
    OutputDataDf
  }

  def processIntentionalData(config: ConfigValues, sparkSession: SparkSession, refineZoneTempTableName: String, subjectMap: Map[String, Int], studyMap : Map[String, Int], deviceMap : Map[String, Int], mobileMap : Map[String, Int], eventMap : Map[String, Int], allocationTableMap: Map[String, String], dataTypeList: ListBuffer[String]): ListBuffer[Row] = {
    EtlUtility.notify("Executing function processIntentionalData() - Start")
    val startTime = System.currentTimeMillis()    
    var errorRowList = new ListBuffer[Row]()
    if(dataTypeList.contains(config.data_type_meal) || dataTypeList.contains(config.data_type_migraine)){
    
    var intentionalDataQuery = ""
    if(dataTypeList.contains(config.data_type_meal) && dataTypeList.contains(config.data_type_migraine)) {
      EtlUtility.notify("Both " + config.data_type_meal + " & " + config.data_type_migraine + " records available!")
      intentionalDataQuery = "select data_type_p_col, study_type, subject_id, watch_unique_id, phone_unique_id, start_time, stop_time, severity_value from " + refineZoneTempTableName + " where data_type_p_col='migraine' or data_type_p_col='meal'"
    } else if(dataTypeList.contains(config.data_type_meal) && !dataTypeList.contains(config.data_type_migraine)) {
      EtlUtility.notify(config.data_type_meal + " records available!")
      intentionalDataQuery = "select data_type_p_col, study_type, subject_id, watch_unique_id, phone_unique_id, start_time, stop_time from " + refineZoneTempTableName + " where data_type_p_col='migraine' or data_type_p_col='meal'"
    } else if(!dataTypeList.contains(config.data_type_meal) && dataTypeList.contains(config.data_type_migraine)) {
      EtlUtility.notify(config.data_type_migraine + " records available!")
      intentionalDataQuery = "select data_type_p_col, study_type, subject_id, watch_unique_id, phone_unique_id, start_time, stop_time, severity_value from " + refineZoneTempTableName + " where data_type_p_col='migraine' or data_type_p_col='meal'"      
    }
      
    val intentionalDataDF = sparkSession.sql(intentionalDataQuery)
//    EtlUtility.notify("intentionalDataDF.count()>> " + intentionalDataDF.count())
//    intentionalDataDF.printSchema()
//    intentionalDataDF.show()
    
    val f_intentional_schema = new StructType()
                          .add("evtypid", "int")
                          .add("studyid", "int")
                          .add("siteid", "int")
                          .add("subjectid", "int")
                          .add("deviceid", "int")
                          .add("mobileid", "int")
                          .add("starttimestamp", "timestamp")
                          .add("endtimestamp", "timestamp")
                          .add("value", "int")
    
    var rowList = new ListBuffer[Row]()
    
    intentionalDataDF.collect().foreach { row =>
        val data_type_p_col = row(0)
//        EtlUtility.notify("data_type_p_col: " + data_type_p_col)
        val study_type = row(1)
//        EtlUtility.notify("study_type: " + study_type)
        val subject_id = row(2)
//        EtlUtility.notify("subject_id: " + subject_id)
        val watch_unique_id = row(3)
//        EtlUtility.notify("watch_unique_id: " + watch_unique_id)
        val phone_unique_id = row(4)
//        EtlUtility.notify("phone_unique_id: " + phone_unique_id)
        val start_time = row(5)
//        EtlUtility.notify("start_time: " + start_time)
        val stop_time = row(6)
//        EtlUtility.notify("stop_time: " + stop_time)
        var severity_value = -1
        if(data_type_p_col.equals(config.data_type_migraine)) {
//          EtlUtility.notify("severity_value: " + row(7))
          severity_value = Integer.parseInt(row(7).toString())
        }       
        
//        EtlUtility.notify("Key:")
//        EtlUtility.notify("data_type_p_col: " + data_type_p_col)
//        EtlUtility.notify("study_type: " + study_type)
//        EtlUtility.notify("subject_id: " + subject_id)
//        EtlUtility.notify("watch_unique_id: " + watch_unique_id)
//        EtlUtility.notify("phone_unique_id: " + phone_unique_id)
//        EtlUtility.notify("start_time: " + start_time)
//        EtlUtility.notify("stop_time: " + stop_time)
//        EtlUtility.notify("severity_value: " + severity_value)
        
//        EtlUtility.notify("Value:")
        val evtypid = eventMap.get(data_type_p_col + "").getOrElse(0)
        val studyid = studyMap.get(study_type + "").getOrElse(0)
        val subjectid = subjectMap.get(subject_id + "").getOrElse(0)
        val deviceid = deviceMap.get(watch_unique_id + "").getOrElse(0)
        val mobileid = mobileMap.get(phone_unique_id + "").getOrElse(0)
        
//        EtlUtility.notify("evtypid: " + evtypid)
//        EtlUtility.notify("studyid: " + studyid)
//        EtlUtility.notify("subjectid: " + subjectid)
//        EtlUtility.notify("deviceid: " + deviceid)
//        EtlUtility.notify("mobileid: " + mobileid)
        val dataCaptureDate = convertStringToDate(start_time.toString())
        val siteid = getSiteId(sparkSession, allocationTableMap, subjectid, studyid, deviceid, mobileid, dataCaptureDate)
        
        if(siteid != 0) {
          val outputRow = Row(evtypid, studyid, siteid, subjectid, deviceid, mobileid, start_time, stop_time, severity_value)
          rowList += outputRow
        } else {
          val errorRow = Row(mobileid, studyid, subjectid, deviceid, 1)
          errorRowList += errorRow
        }
    }
    
    if(rowList.length > 0) {
      val finalData = rowList.toSeq
  
      val finalDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(finalData), f_intentional_schema)
//      EtlUtility.notify("Intentional Data finalDf: ")
//      finalDf.show()
//      EtlUtility.notify("Intentional Data finalDf Schema: ")
//      finalDf.printSchema()
      
      finalDf.coalesce(1).write.mode(SaveMode.Append).jdbc(config.DbUrl, "lilly_ddr_schema.f_intentional", EtlUtility.prepareConProperties(config))
    }
    }
    val endTime = System.currentTimeMillis()
    EtlUtility.notify("Execution Time of function processIntentionalData (in seconds): " + ((endTime - startTime)/1000))
    EtlUtility.notify("Executing function processIntentionalData() - Completed")
    errorRowList
  }
  
  def processPassiveData(config: ConfigValues, sparkSession: SparkSession, refineZoneTempTableName: String, subjectMap: Map[String, Int], studyMap : Map[String, Int], deviceMap : Map[String, Int], mobileMap : Map[String, Int], sensorMap : Map[String, Int], aggMap : Map[String, Int], allocationTableMap: Map[String, String], dataTypeList: ListBuffer[String], refineZonePath: String): ListBuffer[Row] = {
    EtlUtility.notify("Executing function processPassiveData() - Start")
    val startTime = System.currentTimeMillis()
    var errorRowList = new ListBuffer[Row]()
    if(dataTypeList.contains(config.data_type_accelerometer) || dataTypeList.contains(config.data_type_gyro) || dataTypeList.contains(config.data_type_heart_rate)){
    val dateTimeTableMap = createDateTimeTableMap(config, sparkSession)
    
    val passiveDataQuery = "select * from " + refineZoneTempTableName + " where data_type_p_col<>'migraine' and data_type_p_col<>'meal'"
    val passiveDataDF = sparkSession.sql(passiveDataQuery)
//    EtlUtility.notify("passiveDataDF.count()>> " + passiveDataDF.count())
//    passiveDataDF.printSchema()
//    passiveDataDF.show()
    passiveDataDF.createOrReplaceTempView("passiveDataTable")

//    val heartRateDataQuery = "select * from " + refineZoneTempTableName + " where data_type_p_col='heart_rate'"
//    val heartRateDataDf = sparkSession.sql(heartRateDataQuery)
    val heartRateDataTempTableName = "heartRateDataTable"
    if(dataTypeList.contains("heart_rate")) {
      val allRefineDataDf = sparkSession.read.option("mergeSchema", "true").parquet(refineZonePath)
      val allRefineDataTableName = "allRefineDataTable"
      allRefineDataDf.createOrReplaceTempView(allRefineDataTableName)
      val heartRateDataQuery = "select * from " + allRefineDataTableName + " where data_type_p_col='heart_rate'"
      val heartRateDataDf = sparkSession.sql(heartRateDataQuery)
      heartRateDataDf.createOrReplaceTempView(heartRateDataTempTableName)
      cacheTempTable(heartRateDataTempTableName, sparkSession)
    }
    
    val f_passive_schema = new StructType()
                      .add("aggtypid", "int")
                      .add("sensorid", "int")
                      .add("deviceid", "int")
                      .add("subjectid", "int")
                      .add("siteid", "int")
                      .add("studyid", "int")
                      .add("timeid", "int")
                      .add("mobileid", "int")
                      .add("agg_value", "double")
    
    val groupOfRecordsDateWise = sparkSession.sql("select data_type_p_col, study_type, subject_id, watch_unique_id, phone_unique_id, year_p_col, month_p_col, day_p_col, hour_p_col, count(*) as rec_count from passiveDataTable group by data_type_p_col, study_type, subject_id, watch_unique_id, phone_unique_id, year_p_col, month_p_col, day_p_col, hour_p_col")
//    groupOfRecordsDateWise.show()
    
//    var rowList = new ListBuffer[Row]()
    var insertStatementList = new ListBuffer[String]()
    
    groupOfRecordsDateWise.collect().foreach { row =>
        val data_type_p_col = row(0)
        val study_type = row(1)
        val subject_id = row(2)
        val watch_unique_id = row(3)
        val phone_unique_id = row(4)
        val year_p_col = row(5)
        val month_p_col = row(6)
        val day_p_col = row(7)
        val hour_p_col = row(8)
        val rec_count = Integer.parseInt(row(9).toString())
        
        val timeid = dateTimeTableMap.get(year_p_col.toString() + month_p_col.toString() + day_p_col.toString() + hour_p_col.toString()).getOrElse(0)
        EtlUtility.notify("timeid for this date: " + timeid)        
        
        var agg_value = 0.0
        if (data_type_p_col.equals("heart_rate")) {
          
          var avgHeartBeatCountQuery = "select avg(heart_beat_count) from " + heartRateDataTempTableName + " where " + 
                                                              "study_type='" + study_type +
                                                              "' And subject_id='" + subject_id +
                                                              "' And watch_unique_id='" + watch_unique_id +
                                                              "' And phone_unique_id='" + phone_unique_id +
                                                              "' And year_p_col=" + year_p_col + 
                                                              " And month_p_col=" + month_p_col + 
                                                              " And day_p_col=" + day_p_col + 
                                                              " And hour_p_col=" + hour_p_col
                                                                      
          val avgHeartBeatCountDf = sparkSession.sql(avgHeartBeatCountQuery)
          agg_value = avgHeartBeatCountDf.head().getDouble(0)
          EtlUtility.notify("Average Heart Rate: " + agg_value)
          
        } else if (data_type_p_col.equals("accelerometer") || data_type_p_col.equals("gyro")) {
          if(rec_count > 0){
            agg_value = 1
          } else {
            agg_value = 0
          }
        }
        
        val sensorid = sensorMap.get(data_type_p_col + "").getOrElse(0)
        var aggtypid = 1 // Average For heart_rate
        if(sensorid == 1 || sensorid == 2){
            aggtypid = 2 // Existence For accelerometer / gyro          
        }
        val studyid = studyMap.get(study_type + "").getOrElse(0)
        val subjectid = subjectMap.get(subject_id + "").getOrElse(0)
        val deviceid = deviceMap.get(watch_unique_id + "").getOrElse(0)
        val mobileid = mobileMap.get(phone_unique_id + "").getOrElse(0)
        
        val dataCaptureDate = convertStringToDate(year_p_col + "-" + month_p_col + "-" + day_p_col)
        val siteid = getSiteId(sparkSession, allocationTableMap, subjectid, studyid, deviceid, mobileid, dataCaptureDate)

        if(siteid != 0) {
//          val outputRow = Row(aggtypid, sensorid, deviceid, subjectid, siteid, studyid, timeid, mobileid, agg_value)
//          rowList += outputRow
          
          val upsertStatement = "INSERT INTO lilly_ddr_schema.f_passive (aggtypid, sensorid, deviceid, subjectid, siteid, studyid, timeid, mobileid, agg_value) VALUES (" +
                                      aggtypid +
                                 ", " + sensorid +
                                 ", " + deviceid +
                                 ", " + subjectid +
                                 ", " + siteid +
                                 ", " + studyid +
                                 ", " + timeid +
                                 ", " + mobileid +
                                 ", " + agg_value +
                                 ") on conflict (aggtypid, sensorid, deviceid, subjectid, siteid, studyid, timeid, mobileid) do update set agg_value = excluded.agg_value;"
          insertStatementList += upsertStatement
        } else {
          val errorRow = Row(mobileid, studyid, subjectid, deviceid, rec_count)
          errorRowList += errorRow
        }
    }

    if(dataTypeList.contains("heart_rate")) {
      uncacheTempTable(heartRateDataTempTableName, sparkSession)
    }
    
    if(insertStatementList.length > 0) {
//      val finalData = rowList.toSeq
  
//      val finalDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(finalData), f_passive_schema)
  
//      EtlUtility.notify("Passive Data finalDf: ")
//      finalDf.show()
//      EtlUtility.notify("Passive Data finalDf Schema: ")
//      finalDf.printSchema()
      
//      finalDf.coalesce(1).write.mode(SaveMode.Append).jdbc(config.DbUrl, "lilly_ddr_schema.f_passive", EtlUtility.prepareConProperties(config))
      EtlUtility.notify("insertStatementList.length: " + insertStatementList.length)
      JdbcUtility.populatePassiveFactTable(config, insertStatementList)
    }
    }
    val endTime = System.currentTimeMillis()
    EtlUtility.notify("Execution Time of function processPassiveData (in seconds): " + ((endTime - startTime)/1000))
    EtlUtility.notify("Executing function processPassiveData() - Completed")
    errorRowList
  }

  def createDateTimeTableMap(config: ConfigValues, sparkSession: SparkSession): Map[String, Int] = {
    EtlUtility.notify("Executing function createDateTimeTableMap() - Start")
    var dateTimeTableDf = sparkSession.read.jdbc(config.DbUrl, "lilly_ddr_schema.d_datetime", EtlUtility.prepareConProperties(config))
    
    var dateTimeTableMap = Map[String, Int]()
    dateTimeTableDf.collect().foreach{ row =>
      var key = row(1).toString() + row(2).toString() + row(3).toString() + row(4).toString()
      var value = Integer.parseInt(row(0).toString())
      dateTimeTableMap += (key -> value)
    }
    
    EtlUtility.notify("Executing function createDateTimeTableMap() - Completed")
    dateTimeTableMap
  }
    
  def createAllocationTableMap(config: ConfigValues, sparkSession: SparkSession): Map[String, String] = {
    EtlUtility.notify("Executing function createAllocationTempTable() - Start")
    var allocationTableDf = sparkSession.read.jdbc(config.DbUrl, "lilly_ddr_schema.d_allocation", EtlUtility.prepareConProperties(config))
    var allocationTableMap = Map[String, String]()
    allocationTableDf.collect().foreach{ row =>
      var key = row(1).toString() + row(3).toString() + row(4).toString() + row(5).toString()
      val siteid = Integer.parseInt(row(2).toString())
      val startdate = row(6).toString()
      val enddate = row(7).toString()
      val value = siteid + "_" + startdate + "_" + enddate
      allocationTableMap += (key -> value)
    }
    EtlUtility.notify("Executing function createAllocationTempTable() - Completed")
    allocationTableMap
  }
  
  def cacheTempTable(tempTableName: String, sparkSession: SparkSession): Unit = {
    sparkSession.catalog.cacheTable(tempTableName)
  }
    
  def uncacheTempTable(tempTableName: String, sparkSession: SparkSession): Unit = {
    sparkSession.catalog.uncacheTable(tempTableName)
  }

  def getSiteId(sparkSession: SparkSession, allocationTableMap: Map[String, String], subjectid: Int, studyid: Int, deviceid: Int, mobileid: Int, dataCaptureDate : Date): Int = {
//    EtlUtility.notify("Executing function getSiteId() - Start")
    var value = ""
    var siteid = 0
    var startdate: Date = null     
    var enddate: Date = null
    try {
      println(mobileid.toString() + studyid.toString() + subjectid.toString() + deviceid.toString())
      value = allocationTableMap.get(mobileid.toString() + studyid.toString() + subjectid.toString() + deviceid.toString()).get
    } catch {
      case x : Exception => {
        EtlUtility.notify("This Entry is not available in Allocation Table.")        
      }
    }
    if(value != "") {
      var count = 0
      value.toString().split("_").foreach{f =>
        if(count == 0) {
          siteid = Integer.parseInt(f)
          count = count + 1
        } else if(count == 1) {
          startdate = convertStringToDate(f)
          count = count + 1
        } else if(count == 2) {
          enddate = convertStringToDate(f)
          count = count + 1
        }
      }
      
      EtlUtility.notify("siteid for this record: " + siteid)
      EtlUtility.notify("startdate for this record: " + startdate)
      EtlUtility.notify("enddate for this record: " + enddate)
      EtlUtility.notify("dataCaptureDate for this record: " + dataCaptureDate)
      
      if((dataCaptureDate.equals(startdate) || dataCaptureDate.after(startdate)) && (dataCaptureDate.before(enddate) || dataCaptureDate.equals(enddate))){
        EtlUtility.notify("Valid Data as it falls under allocation period.")
      } else {
        EtlUtility.notify("Invalid Data as it not falls under allocation period.")
        siteid = 0
      }
    }
//    EtlUtility.notify("Executing function getSiteId() - Completed")
    siteid
  }

  def populateTempAllocationErrorRecordsTable(sparkSession: SparkSession, config: ConfigValues, errorRowList: List[Row]): Unit = {
    EtlUtility.notify("Executing function populateTempAllocationErrorRecordsTable() - Start")
    val temp_allocation_error_records_schema = new StructType()
                      .add("mobileid", "int")
                      .add("studyid", "int")
                      .add("subjectid", "int")
                      .add("deviceid", "int")
                      .add("reccount", "int")
                          
    val errorData = errorRowList.toSeq

    val errorDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(errorData), temp_allocation_error_records_schema)
    errorDf.createOrReplaceTempView("errorTable")
//    errorDf.show()

    val aggrErrorDataDf = sparkSession.sql("select mobileid, studyid, subjectid, deviceid, count(*) as reccount from errorTable group by mobileid, studyid, subjectid, deviceid")
//    aggrErrorDataDf.show()

    aggrErrorDataDf.coalesce(1).write.mode(SaveMode.Overwrite).jdbc(config.DbUrl, "lilly_ddr_schema.temp_allocation_error_records", EtlUtility.prepareConProperties(config))
    EtlUtility.notify("Executing function populateTempAllocationErrorRecordsTable() - Completed")
  }
  
  def convertStringToDate(value: String): Date = {
    val dataCaptureDate = dateFormat.parse(value)
    dataCaptureDate
  }
  
}