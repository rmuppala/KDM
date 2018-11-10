import java.io.{File, PrintWriter}
import Onto_elem_constructor.canonicalization1
import org.apache.spark.{SparkConf, SparkContext}

object MedicalWords {


    def main(args: Array[String]) {
      System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

      val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
      val sc = new SparkContext(sparkConf)

      val input = sc.textFile("data/tripletnew.txt")
      val triplist = input.map(w => w.split(","))

      val sub1 = triplist.map(t => (t(1), t(0)))
      val sub1res = sub1.groupByKey()
      val pwsa = new PrintWriter(new File("data/subnew_abstract.txt"))
      sub1res.collect().foreach(s => {
        pwsa.write(s._1 + "," + s._2.toList.distinct + "\n")
      })

      pwsa.close()

      val pred1 = triplist.map(t => (t(2), t(0)))
      val pred1res = pred1.groupByKey()


      val pwpa = new PrintWriter(new File("data/prednew_abstract.txt"))
      pred1res.collect().foreach(p => {
        pwpa.write(p._1 + "," + p._2.toList.distinct + "\n")
      })
      pwpa.close()
    }

}
