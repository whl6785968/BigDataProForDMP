object ManyTest {
  def main(args: Array[String]): Unit = {
    val strings = new collection.mutable.ListBuffer[String]()

    strings.append("1")

    strings.append("2")

   /* strings.foreach(println)*/
    println(strings(0))
  }
}
