

package edu.usma.cc

import scala.io.Source
import java.io._
import java.net.URI

import com.martinkl.warc.WARCFileReader
import com.martinkl.warc.WARCFileWriter
import com.martinkl.warc.mapreduce.WARCInputFormat
import com.martinkl.warc.mapreduce.WARCOutputFormat
import com.martinkl.warc.WARCRecord
import com.martinkl.warc.WARCRecord.Header
import com.martinkl.warc.WARCWritable

import org.apache.spark._
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.io._
import scala.collection.mutable.ArrayBuffer



object SimpleApp {import org.apache.spark.sql.functions._


  def main(args: Array[String]) {

    val warcPathFirstHalf = "s3://commoncrawl/"

    println("Starting cluster")

    // Initialize the sparkSession
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext
    
    val source = sc.textFile("s3://eecs-practice/spark-test/wet2018.paths")
    val length = source.count().toInt
    val lineArray = source.take(1000)

    val finalRDD = sc.union(lineArray.map(newPath => sc.newAPIHadoopFile(warcPathFirstHalf + newPath, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]).values))
   
   val newDF = finalRDD.flatMap( warc => analyze4(warc.getRecord)).filter(tup => tup._2 != null).toDF("email","url")
    val reducedDF = newDF.groupBy("email").agg(concat_ws(",", collect_set("url")) as "pageString")

    val savedFilePath = "s3://eecs-practice/spark_test/test_ic2"
    
    reducedDF.rdd.repartition(1).saveAsTextFile(savedFilePath)
    //reducedDF.rdd.saveAsTextFile(savedFilePath)
    //reducedDF.write.save(savedFilePath)

    println("--------------")
    println(s"Emails found in WARCRecords saved in $savedFilePath")
    println("--------------")
    spark.stop()
      }
}

  val analyze4: (WARCRecord => Array[Tuple2[String, String]]) = (record: WARCRecord) => {
    val emails = record.toString.split(" ").filter(word => word.length <= 126 && word.contains("@") && word.endsWith(".ic.gov"))
    
    if (emails.isEmpty) {
      Array(("null", null))
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      val url = uri.toURL.getHost()
      for (email <- emails) yield {
        (email, url.toString)
      }
    }
  }

}
