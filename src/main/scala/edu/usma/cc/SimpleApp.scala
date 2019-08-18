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

    val firstDir = "s3://commoncrawl/crawl-data/CC-MAIN-2019-26/segments/1560627997335.70/wet/CC-MAIN-20190615202724-20190615224454-00027.warc.wet.gz"
 
   //"C:/Users/User/Documents/scala_practice/practice/files/CC-MAIN-20190615202724-20190615224454-00027.warc.wet.gz"
    val warcPathFirstHalf = "s3://commoncrawl/"


    // Initialize the sparkSession
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Open the firstDir directory as an RDD of type [(LongWritable, WARCWritable)]
    val warcInput = sc.newAPIHadoopFile(firstDir, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable])
       
    // Isolate only the WARCWritables in this RDD
    var firstWARCs = warcInput.values
    var warcDF = firstWARCs.flatMap( warc => analyze(warc.getRecord)).toDF("email","url").distinct
    warcDF= warcDF.localCheckpoint
  
    val source = sc.textFile("s3://commoncrawltake2/wet.paths")
    val length = source.count().toInt

   val lineArray = source.take(length).drop(1)
   var i = 1
    for(dirPath <-lineArray){
    try{
    i+=1
    val newPath = warcPathFirstHalf + dirPath
    val newValues = sc.newAPIHadoopFile(newPath, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]).values
    val newWarcDF = newValues.flatMap( warc => analyze(warc.getRecord)).toDF("email","url").distinct
    warcDF = warcDF.union(newWarcDF)
    //TODO 
    //Play with the number of partitions and the decision on when to create a localCheckpoint to optimize
    //More checkpoint causes less data to be read in per loop increasing number of loops and time of operation
    //Too few checkpoints will lead to memory errors
    if(i%100==0) warcDF = warcDF.coalesce(120).localCheckpoint

    }  
    catch { case e:Exception => print("error reading this warc file, file skipped")}
    }


  var reducedDF = warcDF.distinct.groupBy("email").agg(concat_ws(",", collect_set("url")) as "pageString")

    val savedFilePath = "s3://commoncrawltake2/mil" 
    reducedDF.rdd.repartition(10).saveAsTextFile(savedFilePath)


    spark.stop()
  }
  def analyze(record: WARCRecord): Array[Tuple2[String, String]] = {

    // TODO: can we make this statically defined or global so we don't have to instantiate a new one every time
    val milEmailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@mail.mil\b""")

    val content = new String(record.getContent)

    val emails = milEmailPattern.findAllMatchIn(content).toArray.map(email => email.toString)
    var final_array:Array[(String,String)]= Array()
    
    if (emails.isEmpty) {
      return Array(("null", null))
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      val url = uri.toURL.getHost().toString
      for (email <- emails)  yield {
        final_array = final_array :+ (email, url)
      }
      }
      final_array
}
}
