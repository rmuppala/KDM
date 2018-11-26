import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

/**
  * Created by Mayanka on 19-06-2017.
  */
object W2V {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
      .set("spark.driver.memory", "6g").set("spark.executor.memory", "6g")

    val sc = new SparkContext(sparkConf)

    //val input = sc.textFile("data/sample").map(line => line.split(" ").toSeq)

    val documents = sc.wholeTextFiles(path = "data/abstracts", minPartitions = 1)

    val input = documents.map(line => line._2.split(" ").toSeq)
    val modelFolder = new File("PerkinsonW2VModel")

    if (modelFolder.exists()) {
      val sameModel = Word2VecModel.load(sc, "PerkinsonW2VModel")
      val synonyms = sameModel.findSynonyms("disease", 40)

      for ((synonym, cosineSimilarity) <- synonyms) {
        println("cosine similarity for disease  " + s"$synonym $cosineSimilarity")
      }

    }
    else {
      val word2vec = new Word2Vec().setVectorSize(1000)

      val model = word2vec.fit(input)
      val synonyms = model.findSynonyms("disease", 40)

      for ((synonym, cosineSimilarity) <- synonyms) {
        println("synoym, cosine similarity to disease " + s"$synonym $cosineSimilarity")
      }

      model.getVectors.foreach(f => println(f._1 + ":" + f._2 + " : " + f._2.length))

      // Save and load model
      model.save(sc, "PerkinsonW2VModel")

    }

  }
}
