package com.alok.projects

class Utils extends Serializable {
      def hasURL(line: String): Boolean = line matches ".*(GET).*"

}
