

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

val analyze4: (WARCRecord => ArrayBuffer[Tuple2[String, String]]) = (record: WARCRecord) => {
    // val emailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}[A-Za-z]{2,63}\b""")
    //val icEmailPattern = new Regex(""" \b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9-\.]{1,63})\.ic\.gov\b""")

    var emails = ArrayBuffer[String]()
    val content = record.getContent

    //val emails = icEmailPattern.findAllMatchIn(content).toArray.map(email => email.toString)
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
