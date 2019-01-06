package com.alok.projects

import org.scalatest.{FlatSpec, Matchers}

class UtilsTest extends FlatSpec with Matchers{
  "A hasURL routine" should "return true if GET is found in the sentence" in {
    val line = "uplherc.upl.com - - [01/Aug/1995:00:00:14 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 304 0"
    val u = new Utils()
    assert(u.hasURL(line) == true)
  }

  it should "return true if POST is found in the sentence" in {
    val line = "ccn.cs.dal.ca - - [01/Aug/1995:06:39:36 -0400] \"POST /cgi-bin/geturlstats.pl HTTP/1.0\" 200 759"
    val u = new Utils()
    assert(u.hasURL(line) == true)
  }

  it should "return true if HEAD is found in the sentence" in {
    val line = "rossi.astro.nwu.edu - - [01/Aug/1995:06:56:42 -0400] \"HEAD /shuttle/missions/sts-70/mission-sts-70.html HTTP/1.0\" 200 0"
    val u = new Utils()
    assert(u.hasURL(line) == true)
  }

  it should "return false if XYZ is found in the sentence" in {
    val line = "rossi.astro.nwu.edu - - [01/Aug/1995:06:56:42 -0400] \"XYZ /shuttle/missions/sts-70/mission-sts-70.html HTTP/1.0\" 200 0"
    val u = new Utils()
    assert(u.hasURL(line) == false)
  }

  "A extractInfo" should "extract correct info from logs" in {
    val line = "rossi.astro.nwu.edu - - [01/Aug/1995:06:56:42 -0400] \"HEAD /shuttle/missions/sts-70/mission-sts-70.html HTTP/1.0\" 200 0"
    val u = new Utils()
    val list_info = u.extractInfo(line)
    list_info(0) should be ("rossi.astro.nwu.edu")
    list_info(1) should be ("01/Aug/1995:06:56:42 -0400")
    list_info(2) should be ("/shuttle/missions/sts-70/mission-sts-70.html")
    list_info(3) should be ("200")
    list_info(4) should be ("0")
  }

}
