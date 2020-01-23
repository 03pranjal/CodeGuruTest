/**
 * *****************************************************************************
 * Copyright : (C) Eli Lilly & Company
 * Project: Digital Data Registry
 * ****************************************************************************
 */
package com.lilly.ddr.pipeline.constant

import java.util.Properties
import scala.collection.mutable.ListBuffer


/**
 * @purpose This object contains the properties in String keys & values format that should be present in the configuration file also
 *
 */
case class ConfigValues(prop: Properties) {

  val DbUrl = prop.getProperty("DbUrl").trim

  val DbUser = prop.getProperty("DbUser").trim
  val DbPass = prop.getProperty("DbPass").trim

  val DbDriver = prop.getProperty("DbDriver").trim
  val SecretKey = prop.getProperty("SecretKey").trim
  val EncAlgorithm = prop.getProperty("EncAlgorithm").trim

  val jobPrcssStat_InProgress = prop.getProperty("jobPrcssStat_InProgress").trim
  val jobPrcssStat_Completed = prop.getProperty("jobPrcssStat_Completed").trim
  val jobPrcssStat_Failed = prop.getProperty("jobPrcssStat_Failed").trim

  val rawZoneStreamingDataLocalPath = prop.getProperty("rawZoneStreamingDataLocalPath").trim
  val rawZoneStreamingDataS3Path = prop.getProperty("rawZoneStreamingDataS3Path").trim

  val refineZoneLocalPath = prop.getProperty("refineZoneLocalPath").trim
  val refineZoneS3Path = prop.getProperty("refineZoneS3Path").trim

  val conformZoneLocalPath = prop.getProperty("conformZoneLocalPath").trim
  val conformZoneS3Path = prop.getProperty("conformZoneS3Path").trim
  
  val data_type_accelerometer = prop.getProperty("data_type_accelerometer").trim
  val data_type_gyro = prop.getProperty("data_type_gyro").trim
  val data_type_heart_rate = prop.getProperty("data_type_heart_rate").trim
  val data_type_migraine = prop.getProperty("data_type_migraine").trim
  val data_type_meal = prop.getProperty("data_type_meal").trim

  val data_type = prop.getProperty("data_type").trim
  val device_model = prop.getProperty("device_model").trim
  val measurement_time = prop.getProperty("measurement_time").trim
  val phone_unique_id = prop.getProperty("phone_unique_id").trim
  val source_type = prop.getProperty("source_type").trim
  val study_type = prop.getProperty("study_type").trim
  val subject_id = prop.getProperty("subject_id").trim
  val system_name = prop.getProperty("system_name").trim
  val system_version = prop.getProperty("system_version").trim
  val watch_unique_id = prop.getProperty("watch_unique_id").trim
  val x_acceleration = prop.getProperty("x_acceleration").trim
  val y_acceleration = prop.getProperty("y_acceleration").trim
  val z_acceleration = prop.getProperty("z_acceleration").trim
  val year_col = prop.getProperty("year_col").trim
  val month_col = prop.getProperty("month_col").trim
  val day_col = prop.getProperty("day_col").trim
  val hour_col = prop.getProperty("hour_col").trim
  val x_gyro = prop.getProperty("x_gyro").trim
  val y_gyro = prop.getProperty("y_gyro").trim
  val z_gyro = prop.getProperty("z_gyro").trim
  val added_to_health_time = prop.getProperty("added_to_health_time").trim
  val data_sync_time = prop.getProperty("data_sync_time").trim
  val heart_beat_count = prop.getProperty("heart_beat_count").trim
  val severity_value = prop.getProperty("severity_value").trim
  val start_time = prop.getProperty("start_time").trim
  val stop_time = prop.getProperty("stop_time").trim
  val study_type_p_col = prop.getProperty("study_type_p_col").trim
  val subject_id_p_col = prop.getProperty("subject_id_p_col").trim
  val data_type_p_col = prop.getProperty("data_type_p_col").trim
  val year_p_col = prop.getProperty("year_p_col").trim
  val month_p_col = prop.getProperty("month_p_col").trim
  val day_p_col = prop.getProperty("day_p_col").trim
  val hour_p_col = prop.getProperty("hour_p_col").trim
  
  val supportedTimeStampFormatInJsonFile = prop.getProperty("supportedTimeStampFormatInJsonFile").trim

  val createDSiteTableQuery = prop.getProperty("createDSiteTableQuery").trim
  val createDSubjectTableQuery = prop.getProperty("createDSubjectTableQuery").trim
  val createDmobileTableQuery = prop.getProperty("createDmobileTableQuery").trim
  val createDDeviceTableQuery = prop.getProperty("createDDeviceTableQuery").trim
  val createDSensorTableQuery = prop.getProperty("createDSensorTableQuery").trim
  val createDStudyTableQuery = prop.getProperty("createDStudyTableQuery").trim
  val createDDateTimeTableQuery = prop.getProperty("createDDateTimeTableQuery").trim
  val createDAllocationTableQuery = prop.getProperty("createDAllocationTableQuery").trim
  val createDEventTypeTableQuery = prop.getProperty("createDEventTypeTableQuery").trim
  val createDAggTypeTableQuery = prop.getProperty("createDAggTypeTableQuery").trim
  val createFIntentionalTableQuery = prop.getProperty("createFIntentionalTableQuery").trim
  val createFPassiveTableQuery = prop.getProperty("createFPassiveTableQuery").trim
  val createTempAllocationTableQuery = prop.getProperty("createTempAllocationTableQuery").trim
  
  val insert_d_aggtypeQuery = prop.getProperty("insert_d_aggtypeQuery").trim
  val insert_d_device_Query = prop.getProperty("insert_d_device_Query").trim
  val insert_d_eventtype_Query = prop.getProperty("insert_d_eventtype_Query").trim
  val insert_d_mobile_Query = prop.getProperty("insert_d_mobile_Query").trim
  val insert_d_sensor_Query = prop.getProperty("insert_d_sensor_Query").trim
  val insert_d_site_Query = prop.getProperty("insert_d_site_Query").trim
  val insert_d_study_Query = prop.getProperty("insert_d_study_Query").trim
  val insert_d_subject_Query = prop.getProperty("insert_d_subject_Query").trim
  val insert_d_allocation_Query = prop.getProperty("insert_d_allocation_Query").trim
}