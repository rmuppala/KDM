import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

import scala.collection.mutable.ListBuffer
object RulesEngine {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val inputf = sc.textFile("output\\Triplets.txt", 4)
    //val inputf = sc.textFile("output\\triplets_pd.txt", 4)
    //inverseof(inputf)
    //symmetry(inputf)
    //transtive(inputf)
    //propertyAxim(inputf)
    //irreflexive(inputf)
    importantFacts(inputf)
  }
  def importantFacts(inputf : RDD[String]): Unit =
  {

    val trip1 = inputf.map(line => line.split(",")).collect()
    val pw = new PrintWriter(new File("rulesout/importantFacts.txt" ))

    trip1.foreach( t1 =>
    {

        if (t1(1).contains("HIP2") )
          pw.write(t1(0) + "," + t1(1) + "," + t1(2) + "\n")
      })
    pw.close()
  }
  def inverseof(inputf : RDD[String]): Unit =
  {

    val trip1 = inputf.map(line => line.split(",")).collect()
    val trip2 = trip1
    val pw = new PrintWriter(new File("rulesout/inverseOf.txt" ))

    trip1.foreach( t1 =>
    {
      trip2.foreach( t2 => {
        if (t1(0).equals(t2(2))  &&  t1(2).equals(t2(0)) && !t1(1).equals(t2(1)) )
          //println(t1(0) + " " + t1(1) + " " + t1(2) + " inverse to " + t2(0) + " " + t2(1) + " " + t2(2))
         pw.write(t1(0) + "," + t1(1) + "," + t1(2) + " INVERSE TO " + t2(0) + "," + t2(1) + "," + t2(2) + "\n")
      })
    })
    pw.close()
  }
  def symmetry(inputf : RDD[String]): Unit =
  {

    val trip1 = inputf.map(line => line.split(",")).collect()
    val trip2 = trip1
    val pw = new PrintWriter(new File("rulesout/symmetry.txt" ))

    trip1.foreach( t1 =>
    {
      trip2.foreach( t2 => {
        if (t1(0).equals(t2(2))  &&  t1(2).equals(t2(0)) && t1(1).equals(t2(1)) )
        //println(t1(0) + " " + t1(1) + " " + t1(2) + " inverse to " + t2(0) + " " + t2(1) + " " + t2(2))
          pw.write(t1(0) + "," + t1(1) + "," + t1(2) + " SYMMETRIC TO " + t2(0) + "," + t2(1) + "," + t2(2) + "\n")
      })
    })
    pw.close()
  }
  def transtive(inputf : RDD[String]): Unit =
  {

    val trip1 = inputf.map(line => line.split(",")).collect()
    val trip2 = trip1
    val pw = new PrintWriter(new File("rulesout/transtive.txt" ))

    trip1.foreach( t1 =>
    {
      trip2.foreach( t2 => {
        if (t1(2).equals(t2(0))   && t1(1).equals(t2(1)) &&  !t1(2).equals(t2(2))) {
          //println(t1(0) + " " + t1(1) + " " + t1(2) + " inverse to " + t2(0) + " " + t2(1) + " " + t2(2))
          pw.write(t1(0) + "," + t1(1) + "," + t1(2) + "\n")
          pw.write(t2(0) + "," + t2(1) + "," + t2(2) + "\n")
          pw.write("Transtive :" + "\n")
          pw.write(t1(0) + "," + t1(1) + "," + t2(2) + "\n")
          pw.write("\n")
        }
      })
    })
    pw.close()
  }
  def propertyAxim(inputf : RDD[String]): Unit =
  {

    val trip1 = inputf.map(line => line.split(",")).collect()
    val trip2 = trip1
    val trip3 = trip1
    val pw = new PrintWriter(new File("rulesout/propertyaxiom.txt" ))

    trip1.foreach( t1 =>
    {
      trip2.foreach( t2 => {
        if (t1(2).equals(t2(0))   && t1(1).equals(t2(1)) &&  !t1(2).equals(t2(2))) {
          trip3.foreach(t3 => {
            if (t1(0).equals(t3(0)) && t2(2).equals(t3(2)) && !t1(1).equals(t3(1)) ) {
              pw.write(t1(0) + "," + t1(1) + "," + t1(2) + "\n")
              pw.write(t2(0) + "," + t2(1) + "," + t2(2) + "\n")
              pw.write("property axiom :" + "\n")
              pw.write(t3(0) + "," + t3(1) + "," + t3(2) + "\n")
              pw.write("\n")
            }
            }
          )
        }
      })
    })

    pw.close()
  }
  def irreflexive(inputf : RDD[String]): Unit =
  {

    val trip1 = inputf.map(line => line.split(",")).collect()
    val trip2 = trip1
    val pw = new PrintWriter(new File("rulesout/irreflexive.txt" ))

    trip1.foreach( t1 =>
    {
      trip2.foreach( t2 => {
        if (t2(2).equals(t2(0)) && t1(0).equals(t2(0)) && !t1(2).equals(t2(2))  && t1(1).equals(t2(1)) ) {
              pw.write(t1(0) + "," + t1(1) + "," + t1(2) + "\n")
              pw.write(t2(0) + "," + t2(1) + "," + t2(2) + "\n")
              pw.write("\n")
        }
      })
    })

    pw.close()
  }
}
