import java.io.File

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mayanka on 19-06-2017.
  */
object W2VProj {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
      .set("spark.driver.memory", "6g").set("spark.executor.memory", "6g")

    val sc = new SparkContext(sparkConf)

    val input = sc.textFile("data/projfolder/bionlp_words.txt").map(line => line)

    //val input = sc.textFile("data/sample").map(line => line.split(" ")).map(word => word)

      val med = input.map(line => {
        line
      })
      med.collect().foreach(w => print(w))
    /*
    val input = sc.textFile("data/sample").map(line => line.split(" ").map( line => {
    //val documents = sc.textFile(path = "data/projfolder/bionlp_words.txt", minPartitions = 1)
    //val medfile = sc.textFile(path = "data/projfolder/bionlp_words.txt").map(line => {

    val modelFolder = new File("PerkinsonW2VModel")

      val bionlp = line.toString()
        //.split(" ")
      println("find similarity for   " + bionlp)
      if (modelFolder.exists()) {
        val sameModel = Word2VecModel.load(sc, "PerkinsonW2VModel")

        val synonyms = sameModel.findSynonyms(bionlp, 5)

        for ((synonym, cosineSimilarity) <- synonyms) {
          println("cosine similarity for   " + bionlp + s"$synonym $cosineSimilarity")
        }

      }
    })) */
  }

}
