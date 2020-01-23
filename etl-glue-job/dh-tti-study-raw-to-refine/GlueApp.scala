import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.apache.spark.sql.SaveMode

object GlueApp {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession = glueContext.getSparkSession
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
    println("## Glue Job Started!")
    
    // For All Data under RAW ZONE - FULL LOAD
    val inputDF = sparkSession.read.json("s3://lly-dh-tti-ddr-dev/raw/*/*/*/*")
    
    // CDC Approach: ToDo: To Identify what all hourly folders are available to process after last run.
    // val inputDF = sparkSession.read.option("basePath", "C:\\Users\\eshan.jain\\Desktop\\Lilly-Batch-Job\\Raw-Zone").
    // json("C:\\Users\\eshan.jain\\Desktop\\Lilly-Batch-Job\\Raw-Zone\\2019\\10\\24\\3", 
    //     "C:\\Users\\eshan.jain\\Desktop\\Lilly-Batch-Job\\Raw-Zone\\2019\\10\\24\\4",
    //     "C:\\Users\\eshan.jain\\Desktop\\Lilly-Batch-Job\\Raw-Zone\\2019\\10\\24\\5")
    ///
    
    println("RAW Zone Files Schema:-")
    inputDF.printSchema()
    println("Raw Zone Input Files Record Count: "+inputDF.count())
    inputDF.show()
    
    inputDF.createOrReplaceTempView("raw_data")
    
    var accelerometer_outputDF = sparkSession.sql("select * from raw_data where data_type = 'accelerometer'")
    accelerometer_outputDF = accelerometer_outputDF.drop("x_gyro")
    accelerometer_outputDF = accelerometer_outputDF.drop("y_gyro")
    accelerometer_outputDF = accelerometer_outputDF.drop("z_gyro")
    accelerometer_outputDF = accelerometer_outputDF.drop("heart_beat_count")
    accelerometer_outputDF = accelerometer_outputDF.drop("added_to_health_time_millis")
    accelerometer_outputDF = accelerometer_outputDF.drop("data_sync_time_millis")
    
    var gyro_outputDF = sparkSession.sql("select * from raw_data where data_type = 'gyro'")
    gyro_outputDF = gyro_outputDF.drop("x_acceleration")
    gyro_outputDF = gyro_outputDF.drop("y_acceleration")
    gyro_outputDF = gyro_outputDF.drop("z_acceleration")
    gyro_outputDF = gyro_outputDF.drop("heart_beat_count")
    gyro_outputDF = gyro_outputDF.drop("added_to_health_time_millis")
    gyro_outputDF = gyro_outputDF.drop("data_sync_time_millis")
    
    var heartRate_outputDF = sparkSession.sql("select * from raw_data where data_type = 'heart_rate'")
    heartRate_outputDF = heartRate_outputDF.drop("x_acceleration")
    heartRate_outputDF = heartRate_outputDF.drop("y_acceleration")
    heartRate_outputDF = heartRate_outputDF.drop("z_acceleration")
    heartRate_outputDF = heartRate_outputDF.drop("x_gyro")
    heartRate_outputDF = heartRate_outputDF.drop("y_gyro")
    heartRate_outputDF = heartRate_outputDF.drop("z_gyro")
    
    println("Writing accelerometer data into Refine Zone Path: 's3://lly-dh-tti-ddr-refine-dev/accelerometer/'")
    accelerometer_outputDF.write.mode(SaveMode.Append).parquet("s3://lly-dh-tti-ddr-refine-dev/accelerometer/")
    println("Writing accelerometer data Completed!")
    
    println("Writing accelerometer data into Refine Zone Path: 's3://lly-dh-tti-ddr-refine-dev/gyro/'")
    gyro_outputDF.write.mode(SaveMode.Append).parquet("s3://lly-dh-tti-ddr-refine-dev/gyro/")
    println("Writing gyro data Completed!")
    
    println("Writing heart rate data into Refine Zone Path: 's3://lly-dh-tti-ddr-refine-dev/heart_rate/'")
    heartRate_outputDF.write.mode(SaveMode.Append).parquet("s3://lly-dh-tti-ddr-refine-dev/heart_rate/")
    println("Writing heart rate data Completed!")
    
    println("## Glue Job Completed!")
    
    sparkSession.close()
    sparkSession.stop
    Job.commit()
  }
}