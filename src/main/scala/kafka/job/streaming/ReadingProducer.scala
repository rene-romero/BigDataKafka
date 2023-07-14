package kafka.job.streaming

import com.sg.wrapper.SparkSessionWrapper
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.functions.{col, from_json, get_json_object, schema_of_json, sequence, split, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ReadingProducer extends App with SparkSessionWrapper {

  private val currentDirectory = new java.io.File(".").getCanonicalPath
  private val kafkaReaderConfig_1 = KafkaReaderConfig_1("localhost:29092", "csv_to_mysql")
  //private val JDBCConfig_1 = JDBCConfig_1(url = "jdbc:postgresql://localhost:5432/test")
  private val jdbcConfig_1 = JDBCConfig_1(url = "jdbc:mysql://localhost:3306/inventory")
  new StreamingJobExecutor_1(spark, kafkaReaderConfig_1, currentDirectory + "/checkpoint/job", jdbcConfig_1 ).execute()
}

//case class JDBCConfig_1(url: String, user: String = "test", password: String = "Test123", tableName: String = "orders_it")
case class JDBCConfig_1(url: String, user: String = "mysqluser", password: String = "mysqlpw", tableName: String = "aviation_data")

case class KafkaReaderConfig_1(kafkaBootstrapServers: String, topics: String, startingOffsets: String = "earliest")

case class StreamingJobConfig_1(checkpointLocation: String, KafkaReaderConfig_1: KafkaReaderConfig_1)

class StreamingJobExecutor_1(spark: SparkSession, KafkaReaderConfig_1: KafkaReaderConfig_1, checkpointLocation: String, JDBCConfig_1: JDBCConfig_1) {

  def execute(): Unit = {

    //reading data and taking specific information from kafka.
    val transformDF_1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaReaderConfig_1.kafkaBootstrapServers)
      .option("subscribe", KafkaReaderConfig_1.topics)
      .option("startingOffsets", KafkaReaderConfig_1.startingOffsets)
      .option("failOnDataLoss", "false")
      .load().selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "topic")

    val transformDF_2 = transformDF_1.select(
      split(col("value"), ",").getItem(0).as("event_id"),
      split(col("value"), ",").getItem(1).as("investigation_type"),
      split(col("value"), ",").getItem(2).as("accident_number"),
      split(col("value"), ",").getItem(3).as("event_date"),
      split(col("value"), ",").getItem(4).as("location_city"),
      split(col("value"), ",").getItem(5).as("location_state"),
      split(col("value"), ",").getItem(6).as("country"),
      split(col("value"), ",").getItem(7).as("latitude"),
      split(col("value"), ",").getItem(8).as("longitude"),
      split(col("value"), ",").getItem(9).as("airport_code"),
      split(col("value"), ",").getItem(10).as("airport_name"),
      split(col("value"), ",").getItem(11).as("injury_severity"),
      split(col("value"), ",").getItem(12).as("aircraft_damage"),
      split(col("value"), ",").getItem(13).as("aircraft_category"),
      split(col("value"), ",").getItem(14).as("registration_number"),
      split(col("value"), ",").getItem(15).as("make"),
      split(col("value"), ",").getItem(16).as("model"),
      split(col("value"), ",").getItem(17).as("amateur_built"),
      split(col("value"), ",").getItem(18).as("number_of_engines"),
      split(col("value"), ",").getItem(19).as("engine_type"),
      split(col("value"), ",").getItem(20).as("far_description"),
      split(col("value"), ",").getItem(21).as("schedule"),
      split(col("value"), ",").getItem(22).as("purpose_of_flight"),
      split(col("value"), ",").getItem(23).as("air_carrier"),
      split(col("value"), ",").getItem(24).as("total_fatal_injuries"),
      split(col("value"), ",").getItem(25).as("total_serious_injuries"),
      split(col("value"), ",").getItem(26).as("total_minor_injuries"),
      split(col("value"), ",").getItem(27).as("total_uninjured"),
      split(col("value"), ",").getItem(28).as("weather_condition"),
      split(col("value"), ",").getItem(29).as("broad_phase_of_flight"),
      split(col("value"), ",").getItem(30).as("report_status"),
      split(col("value"), ",").getItem(31).as("publication_date")
    )

    transformDF_2
      .writeStream
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch { (batchDF: DataFrame, _: Long) => {
        batchDF
          .write
          .format("jdbc")
          .option("url", JDBCConfig_1.url)
          .option("user", JDBCConfig_1.user)
          .option("password", JDBCConfig_1.password)
          .option("driver", "com.mysql.jdbc.Driver")
          .option(JDBCOptions.JDBC_TABLE_NAME, JDBCConfig_1.tableName)
          //.option("StringType", "unspecified")
          .mode(SaveMode.Append)
          .save()
      }
      }.start()
      .awaitTermination()

    /*transformDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()*/
  }
}
