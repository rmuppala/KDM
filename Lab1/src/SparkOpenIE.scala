package openie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

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

    val input = sc.textFile("C:\\Users\\mraje\\Documents\\UMKC\\KDM\\source\\Retrive_abstract\\hay_fever\\abstracts\\1.txt").map(line => {
      //Getting OpenIE Form of the word using lda.CoreNLP

      //val t=CoreNLP.returnTriplets(line)
      //val t=CoreNLP.returnNoun(line)
      //val t=CoreNLP.returnVerb(line)
      val t=CoreNLP.returnNer(line)
      t
    })


      println("No of Verbs " + input.flatMap(line => line.split(",")).count())
     input.collect().foreach(t=>{println(t)})

   // println(CoreNLP.returnTriplets("The dog has a lifespan of upto 10-12 years."))
   // println(input.collect().mkString("\n"))



  }

}
