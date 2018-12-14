
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
import org.apache.spark.sql.functions._

object SimpleApp {



  def main(args: Array[String]) {

    val warcPathFirstHalf = "s3://commoncrawl/"


    // Initialize the sparkSession
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    
    //Load in list of all commoncrawl paths 
    val source = sc.textFile("s3://eecs-practice/spark-test/wet2018.paths")
    
    //get number of paths
    val length = source.count().toInt
    //put it into an Array in order to loop, use a smaller number than length to work on less files
    val lineArray = source.take(length)

    //build up RDD of all records
    val finalRDD = sc.union(lineArray.map(newPath => sc.newAPIHadoopFile(warcPathFirstHalf + newPath, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]).values))
   
    //find emails and convert to dataframe
    val newDF = finalRDD.flatMap( warc => analyze2(warc.getRecord)).filter(tup => tup._2 != null).toDF("email","url")
    val reducedDF = newDF.groupBy("email").agg(concat_ws(",", collect_set("url")) as "pageString")

    val savedFilePath = "s3://eecs-practice/spark_test/test_fin"
    
    reducedDF.rdd.repartition(1).saveAsTextFile(savedFilePath)

    spark.stop()
      }


val analyze2: (WARCRecord => ArrayBuffer[Tuple2[String, String]]) = (record: WARCRecord) => {

    var emails = ArrayBuffer[String]()
    val content = record.getContent

    var sizeread = 0
    var word = ""
    var startingSearch = true
    for (b <- content){
      var bc = b.toChar
      if (startingSearch){
        //if we are at the start of a word
        if (!bc.isWhitespace){
          sizeread +=1
          word += bc
          startingSearch = false
        }

      }
      //if current search longer than email look for next boundary
      else if (sizeread >= 200){
        //check if starting word
        if(bc.isWhitespace){
          word = ""
          sizeread = 0
        }
      }
      //if at pontential start of word
      else if (sizeread == 0){
        //if at start of word
        if(!bc.isWhitespace){
          sizeread +=1
          word +=bc
        }
      }
      //if in middle of word
      else{
        //if another character
        if(!bc.isWhitespace){
          sizeread +=1
          word += bc
        }
        //else if a boundary
        else{
          sizeread = 0
          if(word.endsWith(".ic.gov") && word.contains("@")) emails += word
          word = ""
        }
      }
    }

    if (emails.isEmpty) {
      ArrayBuffer(("null", null))
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      val url = uri.toURL.getHost()
      for (email <- emails) yield {
       (email, url.toString)
      }
    }
  }


}
