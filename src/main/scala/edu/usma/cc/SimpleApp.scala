
package edu.usma.cc



import scala.io.Source
import java.io._
import java.net.URI
import scala.util.matching.Regex

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
import org.apache.spark.sql.functions._


object SimpleApp {

  def main(args: Array[String]) {

    var firstDir = "s3://commoncrawl/crawl-data/CC-MAIN-2019-26/segments/*/wet/*"
      
      //example of full file path
      //"s3://commoncrawl/crawl-data/CC-MAIN-2016-18/segments/1461860106452.21/wet/CC-MAIN-20160428161506-*"



    // Initialize the sparkSession
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    // Open the files as an RDD of type [(LongWritable, WARCWritable)]
    val warcInput = sc.newAPIHadoopFile(firstDir, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]) 
       
    // Isolate only the WARCWritables in this RDD
    var firstWARCs = warcInput.values


    val milEmailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@.ic.gov\b""")

    // pull out the ic.gov email 
    val newRDD = firstWARCs.flatMap( warc => 
      Try{
      analyze(warc.getRecord)
    }.toOption)
    
    //convert to DF, it's easier to do analytics on this datastructure 
    val newDF = newRDD.toDF("email","url")
    val reducedDF = newDF.groupBy("email").agg(concat_ws(",", collect_set("url")) as "pageString")
    
    //consider checkpoint/ caching at this point so you don't recalculate the data frame each time

    val savedFilePath = "s3://commoncrawltake2/ic" 

    reducedDF.rdd.repartition(8).saveAsTextFile(savedFilePath)
    spark.stop()
  }



  def analyze(record: WARCRecord): Array[Tuple2[String, String]] = {

    // TODO: can we make this statically defined or global so we don't have to instantiate a new one every time
    val milEmailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@.ic.gov\b""")

    val content = new String(record.getContent)

    val emails = milEmailPattern.findAllMatchIn(content).toArray.map(email => email.toString)
    var final_array:Array[(String,String)]= Array()
    
    if (emails.isEmpty) {
      return Array(("null", null))
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      url = uri.toURL.getHost().toString
      for (email <- emails)  yield {
        (email, url)
      }
      }
}
