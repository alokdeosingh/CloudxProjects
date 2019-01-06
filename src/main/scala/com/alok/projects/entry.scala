package com.alok.projects

import org.apache.spark.{SparkConf, SparkContext}

object entry{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Analyzing access logs")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val accessLogs = sc.textFile("src/main/resources/NASA_access_log_Aug95.gz")
    val u = new Utils()
    val accessReducedLogs = accessLogs.filter(u.hasURL).persist()



    //Top 10 links
    val accessLogsLinkInfo = accessReducedLogs.map(log => (u.extractInfo(log)(2),1))
    val toplinks = accessLogsLinkInfo.reduceByKey(_+_)
    val topSortedLinks = toplinks.sortBy(x => x._2,false)
    val top10Links = topSortedLinks.take(10)
    println("------------Top 10 Links--------------------")
    top10Links.foreach(println)

    //Top 10 hosts
    val accessLogsHostInfo = accessReducedLogs.map(log => (u.extractInfo(log)(0),1))
    val tophosts = accessLogsHostInfo.reduceByKey(_+_)
    val topSortedHosts = tophosts.sortBy(x => x._2,false)
    val top10Hosts = topSortedHosts.take(10)
    println("------------Top 10 Hosts--------------------")
    top10Hosts.foreach(println)

    //Top traffic days
    val accessLogsTimeInfo = accessReducedLogs.map(log => (u.extractInfo(log)(1).substring(0,6),1))
    val topTrafficDays = accessLogsTimeInfo.reduceByKey(_+_)
    val topSortedDays = topTrafficDays.sortBy(x => x._2,false)
    val top10Days = topSortedDays.take(10)
    println("------------Top 10 Days--------------------")
    top10Days.foreach(println)
    val leastSortedDays = topTrafficDays.sortBy(x => x._2,true)
    val least10Days = leastSortedDays.take(10)
    println("------------Least 10 Days--------------------")
    least10Days.foreach(println)

    //Top traffic hours
    val accessLogsHoursInfo = accessReducedLogs.map(log => (u.extractInfo(log)(1).substring(12,14),1))
    val topTrafficHours = accessLogsHoursInfo.reduceByKey(_+_)
    val topSortedHours = topTrafficHours.sortBy(x => x._2,false)
    val top10Hours = topSortedHours.take(10)
    println("------------Top 10 Hours--------------------")
    top10Hours.foreach(println)
    val leastSortedHours = topTrafficHours.sortBy(x => x._2,true)
    val least10Hours = leastSortedHours.take(10)
    println("------------Least 10 Hours--------------------")
    least10Hours.foreach(println)

    //Top HTML Codes
    val accessLogsRetcodeInfo = accessReducedLogs.map(log => (u.extractInfo(log)(4),1))
    val topReturnCode = accessLogsRetcodeInfo.reduceByKey(_+_)
    val topSortedRetCode = topReturnCode.sortBy(x => x._2,false)
    val top10Codes = topSortedRetCode.take(10)
    println("------------Most number of Return Codes--------------------")
    top10Codes.foreach(println)

    sc.stop()
  }
}
