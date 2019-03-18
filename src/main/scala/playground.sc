import com.alok.projects.Utils

val line = "rossi.astro.nwu.edu - - [01/Aug/1995:06:56:42 -0400] \"HEAD /shuttle/missions/sts-70/mission-sts-70.html HTTP/1.0\" 200 0"
val u = new Utils()
val l = u.extractInfo(line)