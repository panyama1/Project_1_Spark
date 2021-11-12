import scala.io.Source

object File2 {
  def main(args: Array[String]): Unit = {
    /*
    println("Following is the content read:")
    Source.fromFile("password_basic.txt").foreach{
      print
    }

     */
    val password = Source.fromFile("password_basic.txt").getLines().toList
    if(password(0) == "test") {
      println("True")
    }
  }

}
