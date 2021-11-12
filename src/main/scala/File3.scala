import java.io._

object File3 {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("password_basic.txt"))
    writer.write("test")
    writer.close()
  }

}
