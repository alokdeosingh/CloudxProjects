package com.alok.projects

class Utils extends Serializable {
      def hasURL(line: String): Boolean = line matches ".*(GET|POST|HEAD).*"
      def extractInfo(line: String): List[String] = {
            val pattern = "(.*?)\\s-\\s-\\s\\[(.*?)\\].*?(GET\\s|POST\\s|HEAD\\s)(\\/.*?|.*?)(\\sHTTP\\/1.0\"\\s|\\s)(\\d+)\\s(\\d+|-)".r
            line match {
                  case pattern(host,time,reqtype,url,protocol,retcode,bytes) => List(host,time,url,retcode,bytes,reqtype.trim)

                  case _ => List("No Match Found",line,"","","","")
            }
      }
}
