package kafka.job.streaming

import kafka.job.streaming.StreamingJob.currentDirectory

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import scala.io.Source
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.functions.{col, from_json, get_json_object, schema_of_json, sequence, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.sg.wrapper.SparkSessionWrapper
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object KafkaCsvProducer {
  def main(args: Array[String]): Unit = {
    val config: Properties = new Properties()
    config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
    config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](config)

    //file name with path
    val fileName = "src/main/resources/dataset/AviationDataQuery.csv"

    //Kafka input topic name
    val topicName = "csv_to_mysql"

    for (line <- Source.fromFile(fileName).getLines().drop(1)) { // Dropping the column names
      // Extract Key
      val key = line.split(","){0}

      // Prepare the record to send
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, 1, key, line.replace("\"",""))
      //print("| " + line + " |\n")
      //print("| " + key(2) + " |\n")

      // Send to topic
      producer.send(record)
    }

    producer.flush()
    producer.close()
  }
}