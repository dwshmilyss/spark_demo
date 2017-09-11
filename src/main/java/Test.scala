
object Test {
  def main(args: Array[String]) {
    def g(f:(String,String) => String,str1:String,str2:String) = {
      val line = f(str1,str2)
      println(line)
    }

    def g1(f: => A,str1:String,str2:String) = {
//      val line = f
      println(str1)
    }

    val add = (str1:String,str2:String) => str1+" : "+str2
    g1(new A("123"),"a","b")


    var map:Map[String,Int] = Map()
    if (true){
      map.+=("a" -> 1)
      map.+=("b" -> 2)
      println(map.size)
    }
    println(map.size)
  }
}


class A{
  def this(str:String) = {
    this()
    println(str)
  }
}
