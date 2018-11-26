import org.apache.spark.{SparkConf, SparkContext}


object Onto_elem_constructor {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //generateSubClass(sc)
    //generateIndividuals(sc)
    //generateObjectProperties(sc)
    //generateObjectPropertiesNew(sc)
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

  def generateSubClass(sc :SparkContext ): Unit ={

    import java.io._
    /*val classes = sc.textFile("output1\\classes.txt")
    val cso = classes.collect
    val broadcastClasses = sc.broadcast(cso.toSet)
    val pwc = new PrintWriter(new File("output1\\Classes"))
      cso.foreach(line => pwc.write(canonicalization1(line+ "\n")))
    pwc.close()*/
    val subclass = sc.textFile("output1\\subclasses.txt")
    val pws = new PrintWriter(new File("output1\\SubClasses"))

      subclass.collect().foreach(line =>
    {
       val l = line.split(",")
       pws.write(canonicalization1(l(0)) + "," + canonicalization1(l(1)) + "\n")
    })
    pws.close()
  }

  def generateIndividuals(sc :SparkContext ): Unit ={

    val classes = sc.textFile("output1\\classes.txt")
    val cso = classes.collect
    val broadcastClasses = sc.broadcast(cso.toSet)

    val subclass = sc.textFile("output1\\subclasses.txt")
    val gene = subclass.filter(l => l.contains("Gene")).map(l=>l.split(",")).map(w=>w(1)).collect().toList
    val dese = subclass.filter(l => l.contains("Disease")).map(l=>l.split(",")).map(w=>w(1)).collect().toList
    val chem = subclass.filter(l => l.contains("Chemical")).map(l=>l.split(",")).map(w=>w(1)).collect().toList
    val muta = subclass.filter(l => l.contains("Mutation")).map(l=>l.split(",")).map(w=>w(1)).collect().toList


    import java.io._
    val pw = new PrintWriter(new File("output1\\Individuals"))

    val input = sc.textFile("output1\\subnew.txt")


    input.collect.foreach(tp =>
    {
      broadcastClasses.value.foreach( c =>
        if ( tp.contains(c) ) {
          if (gene.contains(c))
          pw.write("Gene" + "," + canonicalization1(tp) + "\n")
          if (dese.contains(c))
            pw.write("Disease" + "," + canonicalization1(tp) + "\n")
          if (chem.contains(c))
            pw.write("Chemical" + "," + canonicalization1(tp) + "\n")
          if (muta.contains(c))
            pw.write("Mutation" + "," + canonicalization1(tp) + "\n")
        }

      )
    }
    )
    pw.close();
  }

  def generateObjectProperties(sc :SparkContext ): Unit ={

    val classes = sc.textFile("output1\\classes.txt")
    val broadcastClasses = sc.broadcast(classes.collect.toSet)

    import java.io._
    //val pb = new PrintWriter(new File("output/bionlpresult.txt" ))
    val pw = new PrintWriter(new File("output1\\ObjectProperties"))

    val input = sc.textFile("output1\\triplets.txt").map(line => line.split(","))

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


  def generateObjectPropertiesNew(sc :SparkContext ): Unit ={

    val classes = sc.textFile("output1\\classes.txt").collect().toList
    val subclass = sc.textFile("output1\\subclasses.txt")

    val gene = subclass.filter(l => l.contains("Gene")).map(l=>l.split(",")).map(w=>w(1)).collect().toList
    val dese = subclass.filter(l => l.contains("Disease")).map(l=>l.split(",")).map(w=>w(1)).collect().toList
    val chem = subclass.filter(l => l.contains("Chemical")).map(l=>l.split(",")).map(w=>w(1)).collect().toList
    val muta = subclass.filter(l => l.contains("Mutation")).map(l=>l.split(",")).map(w=>w(1)).collect().toList


    import java.io._
    //val pb = new PrintWriter(new File("output/bionlpresult.txt" ))
    val pw = new PrintWriter(new File("output1\\ObjectProperties"))

    val input = sc.textFile("output1\\triplets.txt").map(line => line.split(","))

    input.collect.foreach( tp =>
    {
        var Domain = ""
        var Range = ""


        tp(0).split(" ").foreach( f => {
          if (gene.contains(f))
            Domain = "Gene"
          if (dese.contains(f))
            Domain = "Disease"
          if (chem.contains(f))
            Domain = "Chemical"
          if (muta.contains(f))
            Domain = "Mutation"
          println(Domain + "\n")
        })
      tp(2).split(" ").foreach( f => {
        if (gene.contains(f))
          Range = "Gene"
        if (dese.contains(f))
          Range = "Disease"
        if (chem.contains(f))
          Range = "Chemical"
        if (muta.contains(f))
          Range = "Mutation"
        println(Range + "\n")
      })
      if (!Domain.equals("") && !Range.equals(""))
          pw.write(canonicalization2(tp(1)) + "," + Domain + "," + Range + "," + "Func" +"\n")
    }
    )
    pw.close();
  }
  def generateTripletObj(sc :SparkContext ): Unit ={

    import java.io._
    val pw = new PrintWriter(new File("output1\\Triplets"))

    val input = sc.textFile("output1\\triplets.txt").map(line => line.split(","))

    input.collect.foreach( tp =>
    {
      //List spcl = List['","+","_","-"]

      if ( !((tp(0).contains("'") || tp(0).contains("'"))))
      //pw.write((tp(0)) + "," + canonicalization2(tp(1)) + "," + (tp(2)) + "," + "Obj" +"\n")
      pw.write(canonicalization1(tp(0)) + "," + canonicalization2(tp(1)) + "," + canonicalization1(tp(2)) + "," + "Obj" +"\n")

    }
    )
    pw.close();
  }
}
