import edu.stanford.nlp.simple
import edu.stanford.nlp.simple.Document
import org.apache.spark.{SparkConf, SparkContext}

//import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source

object Triplet
{

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //val inputf = sc.wholeTextFiles("data\\sample", 4)

    //val inputf = sc.textFile("data\\sample", 4)

    val inputf = sc.wholeTextFiles("data/abstracts")

    val tripletAbs =  inputf.map(line => {
      //println("filename " + line._1)
      val absname = line._1.split("/")

      val fname = absname(absname.length -1)
      //println("Actual filename " + fname)
      val triplets = getTriplet(fname, line._2)
      triplets
    }
    )

    //val medlist = sc.textFile("data/medlist.txt").distinct()
    val medlist = sc.textFile("output/classes.txt").distinct()
    val broadcastMedList = sc.broadcast(medlist.collect.toSet)


    val triplist = tripletAbs.flatMap(line => line).filter(tp =>
      broadcastMedList.value.exists(tp._2.contains(_)) || broadcastMedList.value.exists(tp._3.contains(_))
        || broadcastMedList.value.exists(tp._4.contains(_)) )

    import java.io._
    val pw = new PrintWriter(new File("data/tripletnew.txt" ))
    val pwt = new PrintWriter(new File("data/triplets.txt" ))
    val pws = new PrintWriter(new File("data/subnew.txt" ))
    val pwp = new PrintWriter(new File("data/prednew.txt" ))
    val pwo = new PrintWriter(new File("data/objnew.txt" ))

    val drplist = triplist.map(line => (line._2, line._3, line._4)).distinct()

    drplist.collect().foreach( t=>  {
      pwt.write(t._1 + "," + t._2 + "," +t._3 + "\n")
    })
      /*val obj = t._3.split(" ").take(3).mkString(" ")
      var i = 0
      for(  i <- 0 to 3  ){
        if(i < obj.size)
           pwt.write( obj(i))
      pw.write("\n")*
      }
      pwt.write(t._1 + "," + t._2 + "," + t._3 + "," + "\n") */


    triplist.collect().foreach(  t => {

      //pw.write(t + "\n")
      pw.write(t._1 + "," + t._2 + "," + t._3 + "," + t._4 + "\n")

    }
    )
    val sub = triplist.map(  t => t._2).distinct()
    sub.collect().foreach(s => {

      //pw.write(t + "\n")
      pws.write(s + "\n")
    }
    )
    val pred = triplist.map(  t => t._3).distinct()
      pred.collect().foreach(s => {

      //pw.write(t + "\n")
      pwp.write(s + "\n")
    }
    )
    val obj = triplist.map(  t => t._4).distinct()
      obj.collect().foreach(s => {

      //pw.write(t + "\n")
        pwo.write(s + "\n")
      pws.write(s + "\n")
    }
    )
    val sub1 = triplist.map(  t => (t._2, t._1) )
    val sub1res = sub1.groupByKey()

    val pwsa = new PrintWriter(new File("data/subnew_abstract.txt" ))
    sub1res.collect().foreach(s => {

      //pw.write(t + "\n")
      pwsa.write(s + "\n")
    }
    )

    pw.close()
    pwt.close()
    pwp.close()
    pws.close()
    pwo.close()
    pwsa.close()
  }



  def getTriplet(absname : String , text: String) :   List [(String,String, String , String)] =
  {

    val doc: Document = new Document(text)
    var triplet: String = ""
    //val lemmas1 = Array[(String, String, String)]
    val lemmas = ListBuffer.empty[(String, String, String, String)]
    import scala.collection.JavaConversions._
    for ( sent : simple.Sentence <- doc.sentences().asScala.toList) { // Will iterate over two sentences
      val l = sent.openie() //util.Collection[Quadruple[String, String, String, Double]]
      val tdata = l.iterator()
      while(tdata.hasNext())
        {
          val temp = tdata.next()
          if(  temp.third.split(" ").size < 4) {
            lemmas += ((absname, temp.first, temp.second, temp.third))
            println((temp.first, temp.second, temp.third))
          }
        }


    }

    return lemmas.toList.distinct


  }
  def get(url: String) = scala.io.Source.fromURL(url).getLines()
}