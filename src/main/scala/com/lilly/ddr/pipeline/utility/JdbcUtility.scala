/**
 * *****************************************************************************
 * Copyright : (C) Eli Lilly & Company
 * Project: Digital Data Registry
 * ****************************************************************************
 */
package com.lilly.ddr.pipeline.utility

import java.sql.{ DriverManager, PreparedStatement, Timestamp, Connection, ResultSet }
import com.lilly.ddr.pipeline.constant.ConfigValues
import scala.util.{ Try, Failure, Success }
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import scala.collection.mutable.ListBuffer

/**
 * It acts as a utility object required for JDBC Operations
 *
 */
object JdbcUtility {

  /**
   * This close resources automatically
   * @param resource
   * @param block :function that uses the resource
   * @return :function that uses the resource
   */
  def autoClose[A <: AutoCloseable, B](resource: A)(block: A => B): B = {
    Try(block(resource)) match {
      case Success(result) =>
        resource.close()
        result
      case Failure(e) =>
        resource.close()
        throw e
    }
  }

  /**
   * This method returns the decrypted db password
   * @param config :ConfigValues
   * @return Decrypted password
   */
  def getDbPass(config: ConfigValues): String = {
    var encryptor = new StandardPBEStringEncryptor();
    encryptor.setPassword(config.SecretKey); // we HAVE TO set a password
    encryptor.setAlgorithm(config.EncAlgorithm); // optionally set the algorithm
//    EtlUtility.notify("Encrypted Password "+ config.DbPass)
    val dbPass = encryptor.decrypt(config.DbPass);
//    EtlUtility.notify("Password After Decryption: "+ dbPass)
    dbPass

  }

  /**
   * This method returns the connection object
   * @param config :ConfigValues
   * @return Java Connection Object
   */
  def getDbCon(config: ConfigValues): Connection = {
    EtlUtility.notify("=====getDbCon=====")
    val dbPass = getDbPass(config)
    Class.forName(config.DbDriver)
    DriverManager.getConnection(config.DbUrl, config.DbUser, dbPass)
  }

  /**
   * This method simply executes the provided query in database
   * @param config: ConfigValues
   * @param query: String
   * @return queryRunStatus: Int
   */
  def executeQuery(config: ConfigValues, query: String): Int = {
    try {
      Class.forName(config.DbDriver)
      autoClose(getDbCon(config)) { con =>
        autoClose(con.prepareStatement(query)) { stmt =>
          val queryRunStatus = stmt.executeUpdate
          queryRunStatus
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace
        val queryRunStatus = -1
        queryRunStatus
    }
  }
  
  def executeTestQuery(config: ConfigValues, query: String, tableName: String): (Boolean, String) = {
    EtlUtility.notify("Executing function executeTestQuery() - Start")
    var isSuccess = true
    var error = ""

    try {
      Class.forName("org.postgresql.Driver")
      autoClose(getDbCon(config)) { con =>
        autoClose(con.createStatement()) { stmt =>
          var rs = stmt.executeQuery(query)
          EtlUtility.notify("Records Available in Table: " + tableName + " =>")
          while (rs.next()) {
            EtlUtility.notify("Row: " + rs.getString(1))
            EtlUtility.notify("Row: " + rs.getString(2))
          }
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace
        error = e.getMessage
        isSuccess = false
    }
    EtlUtility.notify("Executing function executeTestQuery() - Completed")
    (isSuccess, error)
  }
  
  def createAllTables(config: ConfigValues): Unit = {
    
    EtlUtility.notify("Executing function createAllTables() - Start")
    
    var isSuccess = true
    var error = ""

    try {
      Class.forName("org.postgresql.Driver")
      autoClose(getDbCon(config)) { con =>
        autoClose(con.createStatement()) { stmt =>
          
          var result = stmt.executeUpdate(config.createDSiteTableQuery)
          EtlUtility.notify("createDSiteTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createDSubjectTableQuery)
          EtlUtility.notify("createDSubjectTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createDmobileTableQuery)
          EtlUtility.notify("createDmobileTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createDDeviceTableQuery)
          EtlUtility.notify("createDDeviceTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createDSensorTableQuery)
          EtlUtility.notify("createDSensorTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createDStudyTableQuery)
          EtlUtility.notify("createDStudyTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createDDateTimeTableQuery)
          EtlUtility.notify("createDDateTimeTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createDAllocationTableQuery)
          EtlUtility.notify("createDAllocationTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createDEventTypeTableQuery)
          EtlUtility.notify("createDEventTypeTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createDAggTypeTableQuery)
          EtlUtility.notify("createDAggTypeTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createFIntentionalTableQuery)
          EtlUtility.notify("createFIntentionalTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createFPassiveTableQuery)
          EtlUtility.notify("createFPassiveTableQuery>>"+result)
          
          result = stmt.executeUpdate(config.createTempAllocationTableQuery)
          EtlUtility.notify("createTempAllocationTableQuery>>"+result)
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace
        error = e.getMessage
        isSuccess = false
    }
    
    EtlUtility.notify("Executing function createAllTables() - Completed")
  }

  def populateDBDimTables(config: ConfigValues): Unit = {

    EtlUtility.notify("Executing function populateDBDimTables() - Start")
    
    try {
      Class.forName("org.postgresql.Driver")
      autoClose(getDbCon(config)) { con =>
        autoClose(con.createStatement()) { stmt =>
          var result = stmt.executeUpdate(config.insert_d_aggtypeQuery)
          EtlUtility.notify("insert_d_aggtypeQuery>>"+result)
          
          result = stmt.executeUpdate(config.insert_d_device_Query)
          EtlUtility.notify("insert_d_device_Query>>"+result)
          
          result = stmt.executeUpdate(config.insert_d_eventtype_Query)
          EtlUtility.notify("insert_d_eventtype_Query>>"+result)
          
          result = stmt.executeUpdate(config.insert_d_mobile_Query)
          EtlUtility.notify("insert_d_mobile_Query>>"+result)
          
          result = stmt.executeUpdate(config.insert_d_sensor_Query)
          EtlUtility.notify("insert_d_sensor_Query>>"+result)
          
          result = stmt.executeUpdate(config.insert_d_site_Query)
          EtlUtility.notify("insert_d_site_Query>>"+result)
          
          result = stmt.executeUpdate(config.insert_d_study_Query)
          EtlUtility.notify("insert_d_study_Query>>"+result)
          
          result = stmt.executeUpdate(config.insert_d_subject_Query)
          EtlUtility.notify("insert_d_subject_Query>>"+result)
          
          result = stmt.executeUpdate(config.insert_d_allocation_Query)
          EtlUtility.notify("insert_d_allocation_Query>>"+result)
          
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace
    }
    
    EtlUtility.notify("Executing function populateDBDimTables() - Completed")
  }

  def populateDBDateTimeTable(config: ConfigValues): Unit = {
    EtlUtility.notify("Executing function populateDBDateTimeTable() - Start")

    var range = Map(2019 -> Array(10,11,12), 2020 -> Array(1,2,3))
//    var range = Map(2019 -> Array(10))

    var timeId = 1

    try {
      Class.forName("org.postgresql.Driver")
      autoClose(getDbCon(config)) { con =>
        autoClose(con.createStatement()) { stmt =>
          range.keys.foreach { y =>
            EtlUtility.notify("Year = " + y)
            val mList = range(y).toList
            EtlUtility.notify("Month List = " + mList)
            mList.foreach { m =>
//              EtlUtility.notify("Month  = " + m)
              var d = 0
              for (d <- 1 to 31) {
//                EtlUtility.notify("Day  = " + d)
                var h = 0
                for (h <- 0 to 23) {
//                  EtlUtility.notify("hour  = " + h)
                  var insertStatement = "INSERT INTO lilly_ddr_schema.d_datetime ( timeid, year, month, DATE, hour ) VALUES ( <timeid>, <year>, <month>, <DATE>, <hour> );"
                  insertStatement = insertStatement.replace("<timeid>", timeId + "")
                  insertStatement = insertStatement.replace("<year>", y + "")
                  insertStatement = insertStatement.replace("<month>", m + "")
                  insertStatement = insertStatement.replace("<DATE>", d + "")
                  insertStatement = insertStatement.replace("<hour>", h + "")
                  stmt.addBatch(insertStatement)
                  timeId = timeId + 1
                }
              }
            }
          }
          stmt.executeBatch()
//          EtlUtility.notify("timeId>>" + timeId)
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace
    }

    EtlUtility.notify("Executing function populateDBDateTimeTable() - Completed")
  }

  def getMapping(config: ConfigValues, query: String): Map[String, Int] = {
    EtlUtility.notify("Executing function getMapping() - Start")
    var map = Map[String, Int]()

    try {
      Class.forName("org.postgresql.Driver")
      autoClose(getDbCon(config)) { con =>
        autoClose(con.createStatement()) { stmt =>
          var rs = stmt.executeQuery(query)
          while (rs.next()) {
            map += (rs.getString(2) -> rs.getInt(1))
          }
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace
    }
    EtlUtility.notify("Executing function getMapping() - Completed")
    map
  }
  
  def populatePassiveFactTable(config: ConfigValues, insertStatementList: ListBuffer[String]): Unit = {

    EtlUtility.notify("Executing function populateDBDimTables() - Start")
    
    Class.forName("org.postgresql.Driver")
      autoClose(getDbCon(config)) { con =>
        autoClose(con.createStatement()) { stmt =>
          insertStatementList.toList.foreach{insertQuery =>
            try{
              EtlUtility.notify("Insert row in passive table>>" + stmt.executeUpdate(insertQuery))
            } catch {
              case x: Exception => 
              {  
                  println("Exception occured while inserting this row: " + insertQuery)
                  println("Exception Message: " + x.getMessage) 
              } 
            }
          }
        }
    }
    
    EtlUtility.notify("Executing function populateDBDimTables() - Stop")
  }
}