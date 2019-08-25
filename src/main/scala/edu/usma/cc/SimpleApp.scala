package edu.usma.cc

import scala.io.Source
import java.io._
import java.net.URI
import scala.util.matching.Regex

import org.jwat.warc.WarcReaderFactory
import org.jwat.warc.WarcRecord
import org.jwat.warc.WarcReaderCompressed
import org.jwat.common.HttpHeader
import org.jwat.gzip.GzipReader

import org.apache.spark._
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.io._

import org.apache.spark.sql.functions._

import com.amazonaws.services.s3._, model._
import com.amazonaws.auth.BasicAWSCredentials
import collection.JavaConversions._

import java.io.InputStream
import org.apache.commons.io.IOUtils




object SimpleApp {

  def main(args: Array[String]) {

    // Initialize the sparkSession
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext
    import SQLContext.implicits._ 

    val warcPathFirstHalf = "s3://commoncrawl/"
      //"C:/Users/User/Documents/scala_practice/practice/files/"
    val source = sc.textFile("s3://commoncrawltake2/shortWet.paths")
      //"s3://commoncrawltake2/shortWet.paths")

    val bucket = "commoncrawl"
    def s3 = new AmazonS3Client()


    val records = source.repartition(100).map{path => {

    val byteStream = s3.getObject(bucket, path).getObjectContent.asInstanceOf[InputStream]
    val warcReader = WarcReaderFactory.getReader(byteStream)

    //val warcReader = new WarcReaderCompressed(new GzipReader( s3.getObject(bucket, path).getObjectContent.asInstanceOf[InputStream]))
    var records:Array[Tuple2[String, String]] = Array()
    var thisWarcRecord = warcReader.getNextRecord()
    while(thisWarcRecord != null){
      try{
        records = records ++ analyze(IOUtils.toString(thisWarcRecord.getPayloadContent, "UTF-8"), "test@test.com")
        thisWarcRecord = warcReader.getNextRecord()
      }
      catch{case e: Exception => print("skipping file")
        thisWarcRecord = warcReader.getNextRecord()
      }
    }
    records
    }}

    var reducedDF = builtRDD.toDF("email","url").distinct.distinct.groupBy("email").agg(concat_ws(",", collect_set("url")) as "pageString")
    val savedFilePath = "s3://commoncrawltake2/ic_test" 
    reducedDF.rdd.repartition(10).saveAsTextFile(savedFilePath)


    spark.stop()
  }
  def analyze(record: String, requestURI: String): Array[Tuple2[String, String]] = {

    // TODO: can we make this statically defined or global so we don't have to instantiate a new one every time
    val emailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@gmail.com\b""")

    val content = record

    val emails = emailPattern.findAllMatchIn(content).toArray.map(email => email.toString)
    var final_array:Array[(String,String)]= Array()
    
    if (emails.isEmpty) {
      return final_array
    } else {
      val uri = new URI(requestURI)
      val url = uri.toURL.getHost().toString
      for (email <- emails)  yield {
        final_array = final_array :+ (email, url)
      }
      }
      final_array
}

}

