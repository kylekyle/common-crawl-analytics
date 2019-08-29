package edu.usma.cc

import scala.io.Source
import java.io._
import java.net.URI
import scala.util.matching.Regex

import org.jwat.warc.WarcReaderFactory
import org.jwat.warc.WarcRecord
import org.jwat.warc.WarcHeader

import org.apache.spark._
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.io._

import org.apache.spark.sql.functions._

import com.amazonaws.services.s3._

import collection.JavaConversions._

import java.io.InputStream
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.types._




object SimpleApp {

   def analyze(record: String, requestURI: String): Tuple2[String, Set[String]] = {

    // TODO: can we make this statically defined or global so we don't have to instantiate a new one every time
    val icPattern = new Regex("""@[A-Za-z]{3}\.ic\.gov""")
    val fullemailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@[A-Za-z]{3}\.ic\.gov""")

    val emailIndices:List[Int] = icPattern.findAllMatchIn(record).map(_.start).toList
    val possibleEmails:List[String] = emailIndices.map(i => {
      var start = i - 70
      val end = i+7
      if (start < 0) start = 0
      record.substring(start,end)
    })
    val emails:List[String]=possibleEmails.map(possible_email => fullemailPattern.findFirstMatchIn(possible_email).mkString).filter(x => x != "")
    var final_set:Set[String] = Set()
    
    if (emails.isEmpty || requestURI==null) {
      throw new IllegalArgumentException("No emails or request URI was null")
    } else {
      //val uri = new URI(requestURI)
      //val url = uri.toURL.getHost().toString
      val url = requestURI
      for (email <- emails)  yield {
        final_set = final_set + email
      }
      (url, final_set)
      }
  }

  def main(args: Array[String]) {


    // Initialize the sparkSession
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext

    val schema = StructType(Seq(StructField("path", StringType, true)))
    val source = spark.read.option("header", "false").schema(schema).csv("s3://commoncrawltake2/wet.paths")   
    source.cache
    val bucket = "commoncrawl"
    def s3 = new AmazonS3Client()
    val number_of_files = 56000


    val records = source.repartition(number_of_files).map{path => {
    val byteStream = s3.getObject(bucket, path.getString(0)).getObjectContent.asInstanceOf[InputStream]
    val warcReader = WarcReaderFactory.getReader(byteStream)
    var records:Array[Tuple2[String, Set[String]]] = Array()
    var thisWarcRecord = warcReader.getNextRecord()
    while(thisWarcRecord != null){
      try{
        val str = IOUtils.toString(thisWarcRecord.getPayloadContent, "UTF-8")
        val found = analyze(str, thisWarcRecord.header.warcTargetUriStr)
        //print(found)
        records = records :+ found
        thisWarcRecord = warcReader.getNextRecord()
      }
      catch{case e: Exception => 
        thisWarcRecord = warcReader.getNextRecord()
      }
    }
    records
    }}.flatMap(x=>x)
    records.cache
    records.count

    val savedFilePath = "s3://commoncrawltake2/ic_jar" 
    records.rdd.coalesce(1).saveAsTextFile(savedFilePath)


    spark.stop()
  

}
}
