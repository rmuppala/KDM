import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.mllib.feature.{HashingTF, IDF, Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

/**
  * Created by Mayanka on 19-06-2017.
  */
object TF_IDFICP5 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    //Reading the Text File
    //val documents = sc.textFile("data/Article.txt")

    val documents = sc.wholeTextFiles(path = "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\source\\Retrive_abstract\\hay_fever\\medicalwords\\", minPartitions = 1)
    //Getting the Lemmatised form of the words in TextFile
   /* val documentseq = documents.map(f => {
      // val lemmatised = CoreNLP.returnLemma(f)
      val lemmatised = CoreNLP.returnLemma(f._2)
      val splitString = lemmatised.split(" ")
     //val splitString = f._2.split(" ")
       splitString.toSeq
    })*/

    val documentseq = documents.map(line => line._2.split(" ").sliding(2).toSeq)
    //Creating an object of HashingTF Class
    val hashingTF = new HashingTF()

    //Creating Term Frequency of the document
    val tf = hashingTF.transform(documentseq)
    tf.cache()


    val idf = new IDF().fit(tf)

    //Creating Inverse Document Frequency
    val tfidf = idf.transform(tf)

    val tfidfvalues = tfidf.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val values = ff(2).replace("]", "").replace(")", "").split(",")
      values
    })

    val tfidfindex = tfidf.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val indices = ff(1).replace("]", "").replace(")", "").split(",")
      indices
    })

    tfidf.foreach(f => println(f))

    val tfidfData = tfidfindex.zip(tfidfvalues)

    var hm = new HashMap[String, Double]

    tfidfData.collect().foreach(f => {
      hm += f._1 -> f._2.toDouble
    })

    val mapp = sc.broadcast(hm)

    val documentData = documentseq.flatMap(_.toList)
    val dd = documentData.map(f => {
      val i = hashingTF.indexOf(f)
      val h = mapp.value
      (f, h(i.toString))
    })

    val file = new File("NGram_TF_Syn")
    val bw = new BufferedWriter(new FileWriter(file))



    val dd1 = dd.distinct().sortBy(_._2, false)
    println("Get Synonyms for top TF_IDF words")
    dd1.take(10).foreach(f => {
      println("Top TF_IDG Word " + f + f._2)
      bw.write("Top TF_IDG Word " + f + "\n")
      //getWord2VecSynonym(sc, "NGramModel", f._1, bw)
    })
    bw.close()
  }




  def createword2vecModel(sc: SparkContext, documents: RDD[(String, String)], modelName: String): Unit = {
    var input : RDD[Seq[String]] = documents.map(line => line._2.split(" ").toSeq)
    val modelFolder = new File(modelName)
    try {
      if (!modelFolder.exists())
      {
        val word2vec = new Word2Vec().setVectorSize(1000)
        val model = word2vec.fit(input)
        // Save and load model
        model.save(sc, modelName)

      }
    }
    catch {
      case e: IllegalStateException => println(e)

    }
  }



  def getWord2VecSynonym(sc: SparkContext, modelName : String, word: String, bw : BufferedWriter): Unit = {

    val modelFolder = new File(modelName)
    try {
      if (modelFolder.exists()) {
        val sameModel = Word2VecModel.load(sc, modelName)
        val synonyms = sameModel.findSynonyms(word, 40)
        bw.write("Synonyms for " + word + " | ")
        for ((synonym, cosineSimilarity) <- synonyms) {
          //println("cosine similarity for " + word + " " + s"$synonym $cosineSimilarity")
          bw.write(s"$synonym" + " : ")
        }
        bw.write("\n")
      }
    }
    catch {
      case e: IllegalStateException => println(e)

    }
  }
}
