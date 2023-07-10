package kafka.job.streaming

import com.sg.wrapper.SparkSessionWrapper
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.functions.{col, from_json, get_json_object, schema_of_json, sequence, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object StreamingJob extends App with SparkSessionWrapper {

  private val currentDirectory = new java.io.File(".").getCanonicalPath
  private val kafkaReaderConfig = KafkaReaderConfig("localhost:29092", "internal_topic.inventory.aviation_data")
  private val jdbcConfig = JDBCConfig(url = "jdbc:postgresql://localhost:5432/test")
  new StreamingJobExecutor(spark, kafkaReaderConfig, currentDirectory + "/checkpoint/job", jdbcConfig).execute()
}

case class JDBCConfig(url: String, user: String = "test", password: String = "Test123", tableName: String = "main")

case class KafkaReaderConfig(kafkaBootstrapServers: String, topics: String, startingOffsets: String = "earliest")

case class StreamingJobConfig(checkpointLocation: String, kafkaReaderConfig: KafkaReaderConfig)

class StreamingJobExecutor(spark: SparkSession, kafkaReaderConfig: KafkaReaderConfig, checkpointLocation: String, jdbcConfig: JDBCConfig) {

  def execute(): Unit = {

    //reading data and taking specific information from kafka.
    val transformDF_1 = read().selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "topic")

    //applying schema to my data from kafka.
    val schema = StructType(Array(
      StructField("event_id", StringType),
      StructField("investigation_type", StringType),
      StructField("accident_number", StringType),
      StructField("event_date", StringType),
      StructField("location", StringType),
      StructField("country", StringType),
      StructField("latitude", StringType),
      StructField("longitude", StringType),
      StructField("airport_code", StringType),
      StructField("airport_name", StringType),
      StructField("injury_severity", StringType),
      StructField("aircraft_damage", StringType),
      StructField("aircraft_category", StringType),
      StructField("registration_number", StringType),
      StructField("make", StringType),
      StructField("model", StringType),
      StructField("amateur_built", StringType),
      StructField("number_of_engines", StringType),
      StructField("engine_type", StringType),
      StructField("far_description", StringType),
      StructField("schedule", StringType),
      StructField("purpose_of_flight", StringType),
      StructField("air_carrier", StringType),
      StructField("total_fatal_injuries", StringType),
      StructField("total_serious_injuries", StringType),
      StructField("total_minor_injuries", StringType),
      StructField("total_uninjured", StringType),
      StructField("weather_condition", StringType),
      StructField("broad_phase_of_flight", StringType),
      StructField("report_status", StringType),
      StructField("duplication_date", StringType)
    ))

    //Taking the data which we are interested.
    val transformDF_2 = transformDF_1.select(get_json_object(col("value"), "$.payload").alias("payload"))

    //Taking after from json message
    val transformDF_3 = transformDF_2.select(get_json_object(col("payload"), "$.after").alias("after"))
    val transformDF = transformDF_3.withColumn("data", from_json(col("after"), schema)).select("data.*")

    //Reading the data stored in postgres
    /*val postgres_df = spark.read
      .format("jdbc")
      .option("url", jdbcConfig.url)
      .option("user", jdbcConfig.user)
      .option("password", jdbcConfig.password)
      .option("driver", "org.postgresql.Driver")
      .option(JDBCOptions.JDBC_TABLE_NAME, jdbcConfig.tableName)
      .option("dbtable", "orders_it_2")
      .load()

    postgres_df.show()*/

    transformDF
      .writeStream
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch { (batchDF: DataFrame, _: Long) => {
        val df_1 = batchDF.select(
          col("event_id"),
          col("investigation_type"),
          col("accident_number")
        )

        val df_2 = batchDF.select(
          col("event_date"),
          col("location"),
          col("country")
        )




        df_1
          .write
          .format("jdbc")
          .option("url", jdbcConfig.url)
          .option("user", jdbcConfig.user)
          .option("password", jdbcConfig.password)
          .option("driver", "org.postgresql.Driver")
          .option(JDBCOptions.JDBC_TABLE_NAME, "df_1")
          //.option("StringType", "unspecified")
          .mode(SaveMode.Append)
          .save()

        df_2
          .write
          .format("jdbc")
          .option("url", jdbcConfig.url)
          .option("user", jdbcConfig.user)
          .option("password", jdbcConfig.password)
          .option("driver", "org.postgresql.Driver")
          .option(JDBCOptions.JDBC_TABLE_NAME, "df_2")
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

  def read(): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaReaderConfig.kafkaBootstrapServers)
      .option("subscribe", kafkaReaderConfig.topics)
      .option("startingOffsets", kafkaReaderConfig.startingOffsets)
      .option("failOnDataLoss", "false")
      .load()
  }
}
