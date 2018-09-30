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
    val stopWords = stopwordsfile.flatMap(x => x.split(",")).map(_.trim)
    val broadcastStopWords = sc.broadcast(stopWords.collect.toSet)

    val medwordsfile = sc.textFile("data/medicalwords.txt")
    val medWords = medwordsfile.map(x=>x.split('|')).map(x => x(0))

     medWords.collect().foreach(f=> print("medwords " + f + "\n") )
    val broadcastMedWords = sc.broadcast(medWords.collect.toSet)

    // Flatten, collect, and broadcast.



    //create file for medical words
    createMedfile(sc)

    val documents = sc.wholeTextFiles(path = "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\abstractstxt2", minPartitions = 5)
    //Getting the Lemmatised form of the words in TextFile
    val documentseq = documents.map(f => {
     // val lemmatised = CoreNLP.returnLemma(f)
     //val lemmatised = CoreNLP.returnLemma(f._2)
     // val splitString = lemmatised.split(" ")
      val splitString= f._2.split(" ").filter(!broadcastStopWords.value.contains(_)).filter( w => !w.contains(","))

      splitString.toSeq

     /* val words = f._2
      val ngrams = words.split(' ').sliding(2)
      ngrams.toSeq */

    })




    //for (word <- dc.distinct) println(word)

    //dc.foreach(f => println(f))

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
        dd1.take(20).foreach(f => {
          print("TOP TF IDF " + f._1 + ":" + f._2 + " Synonums : ")
          // find sysnonyms word2vec for top 20 tfidf words
          if (!(new java.io.File("data/lab2wv.txt").exists))
          {

            val lab2wv = new PrintWriter(new File("data/lab2wv.txt"))
            val modelFolder = new File("RegModel")

          try {



            if (modelFolder.exists()) {
              val sameModel = Word2VecModel.load(sc, "RegModel")
              val synonyms = sameModel.findSynonyms(f._1, 5)
              if (synonyms != null)
                lab2wv.write(f._1 + " :" )
                for ((synonym, cosineSimilarity) <- synonyms) {
                  //print("Synonums for " + f._1 + " " + s"$synonym $cosineSimilarity")
                  print("Synonums for " + f._1 + " " + s"$synonym")
                  lab2wv.write(  s"$synonym" + "," )
                }
              lab2wv.write( "\n" )
            }
            else {
              val word2vec = new Word2Vec().setVectorSize(1000)

              val model = word2vec.fit(documentseq)
              val synonyms = model.findSynonyms(f._1, 2)

              for ((synonym, cosineSimilarity) <- synonyms) {
                println("synoym, cosine similarity for " + f._1 + s"$synonym $cosineSimilarity")
              }

              model.getVectors.foreach(f => println(f._1 + ":" + f._2 + " : " + f._2.length))

              // Save and load model
              model.save(sc, "RegModel")

            }
          }
          catch
          {case e: Exception =>
          }
            lab2wv.close()
          }

        })

    //val splitString= f._2.split(" ").filter(!broadcastStopWords.value.contains(_)).filter( w => !w.contains(","))
    //Top 5 medical TF_IDF words
    val ddm = documentData.filter(broadcastMedWords.value.contains(_)).map(f => {
      val i = hashingTF.indexOf(f)
      val h = mapp.value
      (f, h(i.toString))
    })
    val ddmd = ddm.distinct().sortBy(_._2, false)
    val medwv = new PrintWriter(new File("data/medfwv.txt"))

    ddmd.take(5).foreach(f => {
      println("TOP Med TF IDF " + f._1 + ":" + f._2  )
      val modelFolder = new File("RegModel")

      try {
        if (modelFolder.exists()) {
          val sameModel = Word2VecModel.load(sc, "RegModel")
          val synonyms = sameModel.findSynonyms(f._1, 5)
          if (synonyms != null)
            medwv.write(f._1 + " :")
          for ((synonym, cosineSimilarity) <- synonyms) {
            //print("Synonums for " + f._1 + " " + s"$synonym $cosineSimilarity")
            print("Synonums for " + f._1 + " " + s"$synonym")
            medwv.write(s"$synonym" + ",")
          }
          medwv.write("\n")
        }
        val medWordls = medwordsfile.map(x => x.split('|')).map(x => {
          if (x(0).equals(f._1))
          {  println("ontologies for high TFIDF medical word " + x(0) + " : "  + x(2))
            x(0) + ":" + x(2)
           // medwv.write("ontologies for high TFIDF medical word " + x(0) + " : "  + x(2) + "\n")

          }
        })
        //medWordls.collect().foreach(f=> medwv.write("medwords " + f + "\n") )


      }
      catch
      {case e: Exception =>
      }
    })
    medwv.close()


  }

  def createMedfile(sc: SparkContext )  = {

    if (!(new java.io.File("data/medicalwords.txt").exists)) {

      val mdfile = new PrintWriter(new File("data/medicalwords.txt"))

      val documents = sc.wholeTextFiles(path = "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\abstractstxt", minPartitions = 5)
      val documentmed = documents.flatMap(e => {

        val a = e._2.split("\\r?\\n")
        val t = a.map(r => {
          val wordlist = r.split(" ").distinct.map(word => {
            //print ("word " +  word)
            val medword = AnnotateText.getMedWords(word)
            //val rec = r.split(",")
            //((rec(1).toInt, rec(0)), rec(6).toLong)
           // val detl = medword.split("|")
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

          //println("LOOK MY WORD " + wordssyn._1)
          if (wordssyn._2 != null &&  !wordssyn._2.equals("|")) {
            println("MEDICAL WORD " + wordssyn._1 + ":" + wordssyn._2 + "\n")
            mdfile.write( wordssyn._1 + "|" + wordssyn._2 + "\n")
           //mdfile.write("Ontology " + wordssyn._1 + "\n");

          }
        })
      })
      mdfile.close()
    } //if
  } //createMedfile



}
