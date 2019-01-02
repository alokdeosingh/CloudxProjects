package com.alok.projects

import org.scalatest.FlatSpec

class UtilsTest extends FlatSpec {
  "A hasURL routine" should "return true if URL is found in the sentence" in {
    val line = "uplherc.upl.com - - [01/Aug/1995:00:00:14 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 304 0"
    val u = new Utils()
    assert(u.hasURL(line) == true)
  }
}
