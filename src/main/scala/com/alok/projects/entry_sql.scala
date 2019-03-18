package com.alok.projects

import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object entry_sql{
  val spark = SparkSession
    .builder()
    .appName("Analyze Logger")
    .config("spark.master","local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    //val path = args(0)
    //val path = "src/main/resources/NASA_access_log_Aug95.gz"
    //logAnalysis(path)
    logAnalysis("src/main/resources/NASA_access_log_Aug95.gz")
  }

  def logAnalysis(path:String):Unit = {
    val (columns, logdf) = read(path)
    val logSummaryDataSet = logSummaryTyped(logdf)
    // 1. Top 10 URLs
    println("----------------Top 10 URLS--------------------")
    top10RequestedUrls(logSummaryDataSet)
    //2. Top 5 hosts
    println("----------------Top 5 hosts--------------------")
    top5hostslogged(logSummaryDataSet)
    //3. Top 5 days of traffic
    println("----------------Top 5 traffic days--------------------")
    top5TrafficDays(logSummaryDataSet)

    //4. Least 5 days of traffic
    println("----------------Least 5 traffic days--------------------")
    least5TrafficDays(logSummaryDataSet)

    //5. Top 5 return codes
    println("----------------Least 5 html codes--------------------")
    top5HtmlCodes(logSummaryDataSet)
  }


  def read(path:String):(List[String],DataFrame) = {

    val rdd = spark.sparkContext.textFile(path)
    val headerColumns = List("Host","Time","URL","Retcode","numberbytesTransferred","ReqType")
    val schema = dfSchema(headerColumns)
    val u = new Utils
    val data =
      rdd
      .filter(u.hasURL)
      .map(u.extractInfo)
      .map(row)

    val dataframe = spark.createDataFrame(data,schema)
    (headerColumns,dataframe)
  }

  def dfSchema(cols: List[String]): StructType = {
    val host = StructField(cols(0),StringType,true)
    val time = StructField(cols(1),StringType,true)
    val url = StructField(cols(2),StringType,true)
    val retcode = StructField(cols(3),StringType,true)
    val bytes = StructField(cols(4),StringType,true)
    val reqtype = StructField(cols(5),StringType,true)
    StructType(List(host,time,url,retcode,bytes,reqtype))

  }

  def row(line: List[String]): Row = {
    val x = List(line(0),line(1),line(2),line(3),line(4),line(5))
    Row.fromSeq(x)
  }

  def logSummaryTyped(logSummaryDF: DataFrame): Dataset[logs] = {
    logSummaryDF.map(r => logs(
      r.getAs[String]("Host"),
      r.getAs[String]("Time"),
      r.getAs[String]("URL"),
      r.getAs[String]("Retcode"),
      r.getAs[String]("numberbytesTransferred"),
      r.getAs[String]("ReqType")
    ))
  }

  def top10RequestedUrls(logDataSet: Dataset[logs]): Unit ={
    logDataSet.where($"reqtype" === "GET").
      groupBy($"url").
      agg(count($"url")).
      as("Count_Requests").
      orderBy($"count(url)".desc).
      show(10)
  }

  def top5hostslogged(logDataSet: Dataset[logs]): Unit ={
    logDataSet.
      groupBy($"host").
      agg(count($"host")).
      as("no_hosts").
      orderBy($"count(host)".desc).
      show(10)
  }

  def top5TrafficDays(logDataSet: Dataset[logs]): Unit ={
    logDataSet.
      select($"time",substring($"time",0,6).as("Day")).
      groupBy($"Day").
      agg(count($"Day")).
      orderBy($"count(Day)".desc).
      show(5)
  }

  def least5TrafficDays(logDataSet: Dataset[logs]): Unit ={
    logDataSet.
      select($"time",substring($"time",0,6).as("Day")).
      groupBy($"Day").
      agg(count($"Day")).
      orderBy($"count(Day)").
      show(5)
  }

  def top5HtmlCodes(logDataSet: Dataset[logs]): Unit ={
    logDataSet.
      groupBy($"retcode").
      agg(count($"retcode")).
      as("count_retcodes").
      orderBy($"count(retcode)".desc).
      show(5)
  }

}

case class logs (
  host: String,
  time: String,
  url:  String,
  retcode: String,
  bytes  : String,
  reqtype: String
                )
