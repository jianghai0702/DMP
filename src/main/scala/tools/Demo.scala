package tools

object Demo {
  def main(args: Array[String]): Unit = {


    val list = List(1,2,3)
    val list2 = List(4,5,6)


    val list3 = list.::(list2)
    println(list3) // List(List(4, 5, 6), 1, 2, 3)


    val list4 = list.+:(list2)
    println(list4)  // List(List(4, 5, 6), 1, 2, 3)


    val list5 = list.++(list2)
    println(list5) // List(1, 2, 3, 4, 5, 6)

    val list6 = list.++:(list2)
    println(list6)  // List(4, 5, 6, 1, 2, 3)


    val list7 = list.:::(list2)
    println(list7) // List(4, 5, 6, 1, 2, 3)



  }
}
