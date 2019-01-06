package com.alok.projects

import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object entry_sql{
  val spark = SparkSession
    .builder()
    .appName("Analyze Logger")
    .config("spark.master","local")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val path = args(0)
    //val path = "src/main/resources/NASA_access_log_Aug95.gz"
    logAnalysis(path)

  }

  def logAnalysis(path:String):Unit ={
    val (columns, logdf) = read(path)
    //1. Top 10 URLs
    val top10URL = top10RequestedUrls(logdf)
    println("----------------Top 10 URLS--------------------")
    top10URL.show()

    //2. Top 5 hosts
    val top5hosts = top5hostslogged(logdf)
    println("----------------Top 5 hosts--------------------")
    top5hosts.show()

    //3. Top 5 days of traffic
    val top5days = top5TrafficDays(logdf)
    println("----------------Top 5 days--------------------")
    top5days.show()

    //4. Least 5 days of traffic
    val least5days = least5TrafficDays(logdf)
    println("----------------Top 5 days--------------------")
    least5days.show()

    //5. Top 5 return codes
    val top5Codes = top5HtmlCodes(logdf)
    println("----------------Top 5 codes--------------------")
    top5Codes.show()

    println(logdf.select($"ReqType").distinct())
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
  def top10RequestedUrls(df:DataFrame): DataFrame ={
    df.filter($"ReqType" === "GET").groupBy("URL").count().orderBy($"count".desc).limit(10)
  }
  def top5hostslogged(df:DataFrame): DataFrame ={
    df.groupBy("host").count().orderBy($"count".desc).limit(5)
  }
  def top5TrafficDays(df:DataFrame): DataFrame ={
    df.select($"Time",substring($"Time",0,6).as("Day"))
      .groupBy($"Day")
      .count()
      .orderBy($"count".desc)
      .limit(5)

  }
  def least5TrafficDays(df:DataFrame): DataFrame ={
    df.select($"Time",substring($"Time",0,6).as("Day"))
      .groupBy($"Day")
      .count()
      .orderBy($"count")
      .limit(5)

  }
  def top5HtmlCodes(df:DataFrame):DataFrame = {
    df.groupBy($"Retcode").count().orderBy($"count".desc).limit(5)
  }
}
