import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by Mayanka on 27-Jun-16.
  */
object SparkOpenIE {

  def main(args: Array[String]) {
    // Configuration
    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")
    val sparkConf = new SparkConf().setAppName("SparkOpenIE").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)


    // For Windows Users



    // Turn off Info Logger for Console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //val input = sc.textFile("data/sentenceSample").map(line => {



    //val input = sc.textFile("C:\\Users\\mraje\\Documents\\UMKC\\KDM\\source\\Retrive_abstract\\hay_fever\\abstracts\\1.txt").map(line => {
    //val input = sc.textFile("data/abstracts/test.txt").map(line => {
    val input = sc.wholeTextFiles("data/abstracts")

    val tripletAbs =  input.map(line => {

      //Getting OpenIE Form of the word using lda.CoreNLP

      val t=CoreNLP.returnTriplets(line._2)
      //val t=CoreNLP.returnNoun(line)
      //val t=CoreNLP.returnVerb(line)
      //val t=CoreNLP.returnNer(line)
      t

    } )


    import java.io._
    val pw = new PrintWriter(new File("data/triplet.txt" ))
    val sw = new PrintWriter(new File("data/subject.txt" ))
    val pr = new PrintWriter(new File("data/predicate.txt" ))
    val ow = new PrintWriter(new File("data/object.txt" ))
    val st = new PrintWriter(new File("data/stastics.txt" ))

     // println("No of triplets " + input.flatMap(line => line.split(",")).count())
    //input.flatMap(line => line.split(",")).foreach(t=>println(t) )

    val triplist = tripletAbs.flatMap(triplet => triplet.split("\\),")).distinct()

     triplist.collect().foreach(line => {
       val triplet = line.toString().substring(2)
       pw.write(triplet + "\n")

     })

    val sub = triplist.map(line => line.substring(2).split(",")).map( f=> f(0)).distinct()
    sub.collect().foreach( word => sw.write( (word.toString()) + "\n"))


    val pred = triplist.map(line => line.substring(2).split(",")).map( f=> f(1)).distinct()
    pred.collect().foreach( word => pr.write((word.toString()) + "\n"))


    val obj = triplist.map(line => line.substring(2).split(",")).map( f=> f(2)).distinct()
    obj.collect().foreach( word => ow.write((word.toString()) + "\n"))



   // println(CoreNLP.returnTriplets("The dog has a lifespan of upto 10-12 years."))
   // println(input.collect().mkString("\n"))


    val nounlist =  input.map(line => {
      val t=CoreNLP.returnNoun(line._2)
      t

    } )

    val verblist =  input.map(line => {

      val t=CoreNLP.returnVerb(line._2)
      //val t=CoreNLP.returnNer(line)
      t

    } )

    val wcount = input.flatMap(line => line._2.split(" ")).distinct()
    val tripcount = triplist.count()
    val ncount = nounlist.flatMap(line => line.split(",")).distinct()

    st.write("Total words " + wcount.count() + "\n")
    st.write("Total triplets " + tripcount + "\n")
    st.write("Total Noun " + ncount + "\n")
    st.write("Total Entities" + sub.count() + "\n")
    st.write("Total Predicate" + pred.count() + "\n")

    sw.close()
    pr.close()
    ow.close()
    pw.close()
    st.close()
  }


  def canonicalization( word:String ) : String = {

    var words = word.split(" ")
    var cword = ""
    var i = 0


    words.foreach( w =>
      {
        if ( i == 0)
          cword += w.substring(0,1).toLowerCase() + w.substring(1)
        else
          cword += "_" +w.substring(0,1).toUpperCase() + w.substring(1)
        i = i + 1
      }
    )

    return cword
  }


}
