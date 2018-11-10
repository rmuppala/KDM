import org.apache.spark.{SparkConf, SparkContext}


object Onto_elem_constructor {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    generateIndividuals(sc)
    generateObjectProperties(sc)
    generateTripletObj(sc)
  }

  //subject
  def canonicalization1( word:String ) : String = {

    var words = word.split(" ")
    var cword = ""
    var i = 0


    words.foreach( w =>
    {
      if ( i == 0)
        cword += w.substring(0,1).toUpperCase() + w.substring(1)
      else
        cword += w.substring(0,1).toUpperCase() + w.substring(1)
      i = i + 1
    }
    )

    return cword
  }

    //predicate
  def canonicalization2( word:String ) : String = {

    var words = word.split(" ")
    var cword = ""
    var i = 0


    words.foreach( w =>
    {
      if ( i == 0)
        cword += w.substring(0,1).toLowerCase() + w.substring(1)
      else
        cword += w.substring(0,1).toUpperCase() + w.substring(1)
      i = i + 1
    }
    )

    return cword
  }

  def generateIndividuals(sc :SparkContext ): Unit ={

    val classes = sc.textFile("output\\classes.txt")
    val cso = classes.collect
    val broadcastClasses = sc.broadcast(cso.toSet)

    val subclass = sc.textFile("output\\SubClasses")


    import java.io._
    //val pb = new PrintWriter(new File("output/bionlpresult.txt" ))
    val pwc = new PrintWriter(new File("output\\Classes"))
    val pw = new PrintWriter(new File("output\\Individuals"))
    val pw1 = new PrintWriter(new File("output\\Subclass"))

    cso.foreach(line => pwc.write(canonicalization1(line+ "\n")))
    pwc.close()
    val input = sc.textFile("data\\subnew.txt")

    input.collect.foreach(tp =>
    {
      broadcastClasses.value.foreach( c =>
        if ( tp.contains(c) ) {
          pw.write(canonicalization1(c) + "," + canonicalization1(tp) + "\n")
        }

      )
    }
    )
    pw.close();
  }

  def generateObjectProperties(sc :SparkContext ): Unit ={

    val classes = sc.textFile("output\\classes.txt")
    val broadcastClasses = sc.broadcast(classes.collect.toSet)

    import java.io._
    //val pb = new PrintWriter(new File("output/bionlpresult.txt" ))
    val pw = new PrintWriter(new File("output\\ObjectProperties"))

    val input = sc.textFile("data\\triplets.txt").map(line => line.split(","))

    input.collect.foreach( tp =>
    {
      broadcastClasses.value.foreach( c =>
        if ( tp(0).contains(c) || tp(2).contains(c) )
          pw.write(canonicalization2(tp(1)) + "," + canonicalization1(tp(0)) + "," + canonicalization1(tp(2)) + "," + "Func" +"\n")
      )

    }
    )
    pw.close();
  }

  def generateTripletObj(sc :SparkContext ): Unit ={



    import java.io._
    //val pb = new PrintWriter(new File("output/bionlpresult.txt" ))
    val pw = new PrintWriter(new File("output\\Triplets"))

    val input = sc.textFile("data\\triplets.txt").map(line => line.split(","))

    input.collect.foreach( tp =>
    {

      pw.write(canonicalization1(tp(0)) + "," + canonicalization2(tp(1)) + "," + canonicalization1(tp(2)) + "," + "Obj" +"\n")

    }
    )
    pw.close();
  }
}
