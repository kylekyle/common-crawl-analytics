package edu.usma.cc

import scala.io.Source
import java.io._
import java.net.URI
import scala.util.matching.Regex

import org.apache.spark._
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.io._

import org.apache.spark.sql.functions._

import com.amazonaws.services.s3._, model._
import com.amazonaws.auth.BasicAWSCredentials
import collection.JavaConversions._

import java.io.InputStream;

import org.archive.io.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

object SimpleApp {

  def main(args: Array[String]) {

    // Initialize the sparkSession
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext

    val warcPathFirstHalf = "s3://commoncrawl/"
      //"C:/Users/User/Documents/scala_practice/practice/files/"
    val source = sc.textFile("s3://commoncrawltake2/shortWet.paths")

    val bucket = "commoncrawl"
    def s3 =new AmazonS3Client()


    val s = source.map { key => {
    val obj = s3.getObject(bucket, key)
    val byteStream = obj.getObjectContent.asInstanceOf[InputStream]
    val warcReader = WarcReaderFactory.getReader(byteStream)
    var records:Array[Object] = Array()
    var thisWarcRecord =warcReader.getNextRecord()
    while(thisWarcRecord != null){
      try{
        records = records :+ IOUtils.toString(thisWarcRecord.getPayloadContent, "UTF-8")
        thisWarcRecord =warcReader.getNextRecord()
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
  def analyze(record: WARCRecord): Option[Array[Tuple2[String, String]]] = {

    // TODO: can we make this statically defined or global so we don't have to instantiate a new one every time
    val emailPattern = new Regex("""\b[A-Za-z0-9._%+-]{1,64}@gmail.com\b""")

    val content = new String(record.getContent)

    val emails = emailPattern.findAllMatchIn(content).toArray.map(email => email.toString)
    var final_array:Array[(String,String)]= Array()
    
    if (emails.isEmpty) {
      return None
    } else {
      val uri = new URI(record.getHeader.getTargetURI)
      val url = uri.toURL.getHost().toString
      for (email <- emails)  yield {
        final_array = final_array :+ (email, url)
      }
      }
      Some(final_array)
}

}








