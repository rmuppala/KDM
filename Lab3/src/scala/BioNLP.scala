import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, PartOfSpeechAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source

object BioNLP
{

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //val inputf = sc.wholeTextFiles("data\\Just_Ids", 4)

    import java.io._
    //val pb = new PrintWriter(new File("output/bionlpresult.txt" ))
    val pw = new PrintWriter(new File("output/medicalwords.txt" ))
    val pw1 = new PrintWriter(new File("output/medlist.txt" ))

      val Bioconcept = "Bioconcept"
      val Inputfile = "data\\Just_Ids"
      var Format = "PubTator"
      if (args.length > 2) Format = args(2)

      val medWords = ListBuffer.empty[(String, String)]
      val medNlpStr = ""
      val medList = ""

      for (line <- Source.fromFile(Inputfile).getLines) {
        val data = get("https://www.ncbi.nlm.nih.gov/CBBresearch/Lu/Demo/RESTful/tmTool.cgi/" + Bioconcept + "/" + line + "/" + Format + "/")

        //first 2 lines abstarts some details
        val lines = data.flatMap(line => {
          line.split("\n")
        }).drop(2)

        //lines.foreach(l => if (l.length() > 0) pb.write(l+ "\n") )

        val words = lines.flatMap(word => {word.split("\t").drop(3).dropRight(1)}).toArray

        for (i <- 0 until words.length)
         {
           //Gene, Disease, Chemical, Species, Mutation
           if (words(i).equals("Gene") ||  words(i).equals("Disease") ||
             words(i).equals("Chemical") || words(i).equals("Species") || words(i).equals("Mutation")) {
             pw.write("," + words(i) + "\n")
             pw1.write("\n")
             medNlpStr.concat( words(i) + "\n")
             medList.concat("\n")
           }
           else
           {
             val word = "Parkinson disease"
             if (words(i).toUpperCase.equals("PD") || words(i).toUpperCase.equals("Parkinson's disease".toUpperCase()))
             {
               pw.write(word + " ")
               pw1.write(word + " ")
               medNlpStr.concat( word + " ")
               medList.concat(word + " ")
             }
             else
             {
               pw.write(words(i) + " ")
               pw1.write(words(i) + " ")
               medNlpStr.concat( words(i) + " ")
               medList.concat(words(i) + " ")
             }



           }

         }

        for(i <- 0 until words.length by 2)
        {
          if(i < words.length - 1)
          {
            val work = (words(i), words(i+1))
            medWords += work
          }
        }

        /*print(medNlpStr)
        val mednlplst = medNlpStr.split("\n").distinct
        mednlplst.foreach(w => print(w))
          //w=> pw.write(w + "\n"))

        val medlst = medList.split("\n")
        medlst.distinct.foreach( w => pw1.write(w + "\n")) */
        //medWords.foreach( s=> println(s._1 + " " + s._2))

        //val medData = sc.parallelize(medWords.toList).distinct()

        //medWords.distinct.foreach( s=> pw.write(s._1 + " " + s._2 + "\n"))
        //medWords.distinct.foreach( s=> pw1.write(s._1  + "\n"))


      }
    val medwordscount = medWords.map( s => s._1).distinct
    pw.write("Medical words " + medWords.distinct.length + "\n" )
    //pb.close()
    pw.close()
    pw1.close()
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