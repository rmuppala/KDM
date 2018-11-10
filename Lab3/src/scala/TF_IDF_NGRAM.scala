import java.io.{File, PrintWriter}

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

/**
  * Created by Mayanka on 19-06-2017.
  */
object TF_IDF_NGRAM {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val eventsfile = sc.textFile("data/subnew.txt")
    val events = eventsfile.map(x=>x.split('|')).map(x => x(0))
    val broadcastEvents = sc.broadcast(events.collect.toSet)

    val medfile = sc.textFile("data/medlist.txt")
    //val medlist = medfile.map(x=>x.split('|')).map(x => x(0))
    val broadcastMedList = sc.broadcast(medfile.collect.toSet)

    //Reading the corpus
    val documents = sc.wholeTextFiles("data/abstracts") //WholeTextFiles


    val docs2grams = documents.map(f =>{
      val dn = getNGrams(f._2, 2)
      val ret = dn.map(a=>{
        val s = a.mkString(" ")
        s
      })
      ret.toSeq
    })

    val docs3grams = documents.map(f=>{
      val dn = getNGrams(f._2, 3)
      val ret = dn.map(a=>{
        var s = a.mkString(" ")
        s
      })
      ret.toSeq
    })

    //1 gram

    val documentseq = documents.map(f => {
      val splitString= f._2.split(" ")
      splitString.toSeq
    })

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

    val tfidfData1 = tfidfindex.zip(tfidfvalues)

    var hm = new HashMap[String, Double]

    tfidfData1.collect().foreach(f => {
      hm += f._1 -> f._2.toDouble
    })

    val mapp = sc.broadcast(hm)

    val documentData = documentseq.flatMap(_.toList)
    val dd = documentData.map(f => {
      val i = hashingTF.indexOf(f)
      val h = mapp.value
      (f, h(i.toString))
    })
    import java.io._
    val pw = new PrintWriter(new File("output/tfidfmedlist1.txt" ))
    val pws = new PrintWriter(new File("output/tfidfsub1.txt" ))


    //val dd1 = dd.distinct().sortBy(_._2, false)
    val ddval = dd.distinct().sortBy(_._2, false)
    val dd1 = ddval.filter(l => broadcastMedList.value.contains(l._1))
    val dde = ddval.filter(l => broadcastEvents.value.contains(l._1))

    ddval.take(20).foreach(f => {
      //pw1.write(f + "\n")
    })
    dde.take(100).foreach(f => {
      //pws.write(f + "\n")
      pws.write(f._1 + "\n")
    })
    dd1.take(100).foreach(f => {
      //pw.write(f + "\n")
      pw.write(f._1 + "\n")
    })

    //here are the 2grams

    val temp2 = docs2grams

   val hashingTF2 = new HashingTF()

   val tf2 = hashingTF2.transform(docs2grams)
   tf2.cache()

    val idf2 = new IDF().fit(tf2)

    //Creating Inverse Document Frequency
    val tfidf2 = idf2.transform(tf2)

    val tfidf2values = tfidf2.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val values = ff(2).replace("]", "").replace(")", "").split(",")
      values
    })

    val tfidf2index = tfidf2.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val indices = ff(1).replace("]", "").replace(")", "").split(",")
      indices
    })


    val tfidfData = tfidf2index.zip(tfidf2values)

    var hm2 = new HashMap[String, Double]

    tfidfData.collect().foreach(f => {
      hm2 += f._1 -> f._2.toDouble
    })

    val mapp2 = sc.broadcast(hm2)

    val documentData2 = docs2grams.flatMap(_.toList)
    val dd2 = documentData2.map(f => {
      val i = hashingTF2.indexOf(f)
      val h = mapp2.value
      (f, h(i.toString))
    })
    val pw2 = new PrintWriter(new File("output/TF_IDF_BigramMed1.txt"))
    var s2:String=""
    val ddval2 = dd2.distinct().sortBy(_._2, false)

    ddval2.take(100).foreach(f => {
      //pw1.write(f + "\n")
    })
    val dd2r = ddval2.filter(l => broadcastMedList.value.contains(l._1))

    dd2r.take(100).foreach(f => {
      //pw.write(f + "\n")
      //pw2.write(f + "\n")
      pw.write(f._1 + "\n")
      pw2.write(f._1 + "\n")
    })




    // and here are the 3grams
    val temp3 = docs3grams

    val hashingTF3 = new HashingTF()

    val tf3 = hashingTF3.transform(docs3grams)
    tf3.cache()

    val idf3 = new IDF().fit(tf3)

    //Creating Inverse Document Frequency
    val tfidf3 = idf3.transform(tf3)

    val tfidf3values = tfidf3.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val values = ff(2).replace("]", "").replace(")", "").split(",")
      values
    })

    val tfidf3index = tfidf3.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val indices = ff(1).replace("]", "").replace(")", "").split(",")
      indices
    })

    val tfidf3Data = tfidf3index.zip(tfidf3values)

    var hm3 = new HashMap[String, Double]

    tfidf3Data.collect().foreach(f => {
      hm3 += f._1 -> f._2.toDouble
    })

    val mapp3 = sc.broadcast(hm3)

    val documentData3 = docs3grams.flatMap(_.toList)
    val dd3 = documentData3.map(f => {
      val i = hashingTF3.indexOf(f)
      val h = mapp3.value
      (f, h(i.toString))
    })

    var s3:String=""
    val pw3 = new PrintWriter(new File("output/TF_IDF_TrigramMed1.txt"))
    val ddval3 = dd3.distinct().sortBy(_._2, false)
    ddval3.take(10).foreach(f => {
      //println(f)
      //pw1.write(f+"\n")
    })
    val dd13 = ddval3.filter(l => broadcastMedList.value.contains(l._1))
    dd13.take(100).foreach(f => {
      //pw3.write(f+"\n")
      //pw.write(f+"\n")
      pw3.write(f._1+"\n")
      pw.write(f._1+"\n")
    })

    pw.close()
    //pw1.close()
    pw2.close()
    pw3.close()
    pws.close()

  }

  def getNGrams(sentence: String, n:Int): Array[Array[String]] = {
    val words = sentence
    val ngrams = words.split(' ').sliding(n)
    ngrams.toArray
  }



}


