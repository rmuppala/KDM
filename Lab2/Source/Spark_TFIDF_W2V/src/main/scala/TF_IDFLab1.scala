import java.io.File
import java.io._

import org.apache.spark.mllib.feature.{HashingTF, IDF, Word2Vec, Word2VecModel}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

/**
  * Created by Mayanka on 19-06-2017.
  */
object TF_IDFLab1 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)


    //Reading the Text File
    //val documents = sc.textFile("data/Article.txt")
    val stopwordsfile = sc.textFile("data/stopwords.txt")



    // Flatten, collect, and broadcast.

    val stopWords = stopwordsfile.flatMap(x => x.split(",")).map(_.trim)
    val broadcastStopWords = sc.broadcast(stopWords.collect.toSet)

    //create file for medical words
    createMedfile(sc)

    val documents = sc.wholeTextFiles(path = "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\abstractstxt", minPartitions = 5)
    //Getting the Lemmatised form of the words in TextFile
/*    val documentseq = documents.map(f => {
     // val lemmatised = CoreNLP.returnLemma(f)
     val lemmatised = CoreNLP.returnLemma(f._2)
     // val splitString = lemmatised.split(" ")
      val splitString= lemmatised.split(" ").filter(!broadcastStopWords.value.contains(_)).filter( w => !w.contains(","))

      splitString.toSeq

     /* val words = f._2
      val ngrams = words.split(' ').sliding(2)
      ngrams.toSeq */

    })*/




    //for (word <- dc.distinct) println(word)

    //dc.foreach(f => println(f))
    /*
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

        val dd1 = dd.distinct().sortBy(_._2, false)
        dd1.take(5).foreach(f => {
          print("TOP TF IDF " + f._1 + ":" + f._2 + " Synonums : ")



          val modelFolder = new File("RegModel")

          if (modelFolder.exists()) {
            val sameModel = Word2VecModel.load(sc, "RegModel")
            val synonyms = sameModel.findSynonyms(f._1, 3)

            for ((synonym, cosineSimilarity) <- synonyms) {
              print( f._1 + " " + s"$synonym $cosineSimilarity")
            }

          }
          else {
            val word2vec = new Word2Vec().setVectorSize(1000)

            val model = word2vec.fit(documentseq)
            val synonyms = model.findSynonyms(f._1, 2)

            for ((synonym, cosineSimilarity) <- synonyms) {
              println("synoym, cosine similarity for "  + f._1 + s"$synonym $cosineSimilarity")
            }

            model.getVectors.foreach(f => println(f._1 + ":" + f._2 + " : " + f._2.length))

            // Save and load model
            model.save(sc, "RegModel")

          }

          })
     raji */
  }

  def createMedfile(sc: SparkContext )  = {

    if (!(new java.io.File("data/medicalwrods.txt").exists)) {

      val mdfile = new PrintWriter(new File("data/medicalwrods.txt"))

      val documents = sc.wholeTextFiles(path = "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\abstractstxt", minPartitions = 5)
      val documentmed = documents.flatMap(e => {

        val a = e._2.split("\\r?\\n")
        val t = a.map(r => {
          val wordlist = r.split(" ").distinct.map(word => {
            //print ("word " +  word)
            val medword = AnnotateText.getMedWords(word)
            //val rec = r.split(",")
            //((rec(1).toInt, rec(0)), rec(6).toLong)
            (word, medword)
          })
          wordlist
        })
        //print("t ..." + t)
        t
      })


      println("Medical WORDS .....")
      documentmed.collect().foreach(linesyn => {
        linesyn.foreach(wordssyn => {
          if (wordssyn._2.length() != 0) {
            println(wordssyn._1 + ":" + wordssyn._2 + "\n")
            mdfile.write(wordssyn._1 + ":" + wordssyn._2 + "\n")
          }
        })
      })
      mdfile.close()
    } //if
  } //createMedfile
}
