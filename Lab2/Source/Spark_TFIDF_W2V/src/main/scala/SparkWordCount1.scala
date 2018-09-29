import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, PartOfSpeechAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object SparkWordCount1 {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

   // val inputf = sc.wholeTextFiles("data\\sample", 4)

    val inputf = sc.wholeTextFiles("C:\\Users\\mraje\\Documents\\UMKC\\KDM\\abstractstxt", 4)

    val lemmatized = inputf.map(line => lemmatize(line._2))
    val flatLemma = lemmatized.flatMap(list => list)

    //val flatInput = inputf.flatMap(doc=>{doc._2.split(" ")})

    val wc = flatLemma.map(word =>(word._1,1))

    val wordnetCount = flatLemma.map(word => if(new RiWordNet("C:\\Users\\mraje\\Documents\\UMKC\\KDM\\WordNet\\2.1").exists(word._1)) (word._1,1) else (word._1, 0))
    val posCount = flatLemma.map(word => (word._2,1))

    val wNetCount = wordnetCount.reduceByKey(_+_)
    wNetCount.saveAsTextFile("outwordNetCount")

    val oPosCount = posCount.reduceByKey(_+_)
    oPosCount.saveAsTextFile("outPos")

    //val input = sc.textFile("input", 4)

    //val wc=input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()

    val output = wc.reduceByKey(_+_)
    output.saveAsTextFile("outWordCount")


  }

  // code referenced from https://stackoverflow.com/questions/30222559/simplest-method-for-text-lemmatization-in-scala-and-spark
  def lemmatize(text: String): ListBuffer[(String, String)] = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)
    val document = new Annotation(text)
    pipeline.annotate(document)

    val lemmas = ListBuffer.empty[(String, String)]
    val sentences = document.get(classOf[SentencesAnnotation])

    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      val pos = token.get(classOf[PartOfSpeechAnnotation])

      if (lemma.length > 1) {
        lemmas += ((lemma.toLowerCase, pos.toLowerCase))
      }
    }
    lemmas
  }

  def get(url: String) = scala.io.Source.fromURL(url).getLines()
}