

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
import scala.collection.mutable.ArrayBuffer



object SimpleApp {import org.apache.spark.sql.functions._


  def main(args: Array[String]) {
    val emailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}[A-Za-z]{2,63}\b""")

    // Path to WARC files
    val firstDir = "s3://commoncrawl/crawl-data/CC-MAIN-2018-47/segments/1542039741016.16/wet/CC-MAIN-20181112172845-20181112194415-00012.warc.wet.gz"
    val warcPathFirstHalf = "s3://commoncrawl/"

    println("Starting cluster")

    // Initialize the sparkSession
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Open the firstDir directory as an RDD of type [(LongWritable, WARCWritable)]
    val warcInput = sc.newAPIHadoopFile(firstDir, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]) 
       
    // Isolate only the WARCWritables in this RDD
    val firstWARCs = warcInput.values
    
    // returns an RDD containing tuples of type (String, Array[String]) which represent an email and the array of pages where it was found sorted from least to greatest number of appearances.

    // Changing things up a little bit to turn our RDD into a DataFrame a little sooner.
    // The DataFrame will have a row for each email, url combo.
    // The groupBy and aggregate (agg function) will create a unique list of URLs, then convert to a String using concat_ws

    var firstRDD = firstWARCs.flatMap(warc => analyze2(warc.getRecord)).filter( tup => tup._2 != null)
    // case class Record(email: String, url: String)
    var firstDF = firstRDD.toDF("email","url")
    var reducedDF = firstDF.groupBy("email").agg(concat_ws(",", collect_set("url")) as "pageString")
    // We may want to just do a collect_set and do something with its size for easier analysis after saving
    //     var reducedDF = firstDF.groupBy("email").agg(collect_set("url") as "pages")
    //     var reducedDF2.withColumn("num_pages",size(col("pages")))

    println(reducedDF.show)

 //   .reduceByKey(_ ++ _).sortBy(_._2.size).map(tup => (tup._1, tup._2.mkString(",")))
    
    
    val source = sc.textFile("s3://eecs-practice/spark-test/wet2018.paths")
    val length = source.count().toInt
    val lineArray = source.take(length).drop(1)
    var i = 0
    for(dirPath <-lineArray){
      val newPath = warcPathFirstHalf + dirPath
      val newInput = sc.newAPIHadoopFile(newPath, classOf[WARCInputFormat], classOf[LongWritable],classOf[WARCWritable]) 
      val newWarcs = newInput.values

      // Creates a new RDD which contains tuples of an email and all of the pages it was found on. 
      val matches = newWarcs.flatMap( warc => analyze2(warc.getRecord) )
      val filtered = matches.filter(tup => tup._2 != null)

      var nextDF = filtered.toDF("email","url")

      var nextreducedDF = nextDF.groupBy("email").agg(concat_ws(",", collect_set("url")) as "pageString")

      reducedDF = reducedDF.unionAll(nextreducedDF)
      
      //create an action so spark executes 
      if (i%10 == 0) print(reducedDF.count + "\n")
    }

    val savedFilePath = "s3://eecs-practice/spark_test/test_14"
    
    reducedDF.rdd.repartition(50).saveAsTextFile(savedFilePath)
    //reducedDF.rdd.saveAsTextFile(savedFilePath)
    //reducedDF.write.save(savedFilePath)

    println("--------------")
    println(s"Emails found in WARCRecords saved in $savedFilePath")
    println("--------------")
    spark.stop()
      }
}

//val icEmailPattern = new Regex(""" \b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9-\.]{1,63})\.ic\.gov\b""")
  def analyze2(record: WARCRecord): ArrayBuffer[Tuple2[String, String]] = {

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
      return ArrayBuffer(("null", null))
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      val url = uri.toURL.getHost()
      for (email <- emails) yield {
       (email, url.toString)
      }
    }
  }

}
