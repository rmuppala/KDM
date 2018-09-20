package wordnet

import java.io._

import openie.CoreNLP
import org.apache.spark.{SparkConf, SparkContext}
import rita.RiWordNet

/**
  * Created by Mayanka on 26-06-2017.
  */
object SparkPos {





  def wordcount(): Unit =
  {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setAppName("WordNetSpark").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)

    val data=sc.textFile("C:\\Users\\mraje\\Documents\\UMKC\\KDM\\source\\Retrive_abstract\\hay_fever\\medicalwords\\1_bp.txt");


    val writer = new PrintWriter(new File("C:\\Users\\mraje\\Documents\\UMKC\\KDM\\source\\Retrive_abstract\\hay_fever\\abstractstxt\\wc10.txt" ))

    val totalwords = data.flatMap(line => line.split(" ")).count()

    writer.write("total words " + totalwords + "\n")
    //word frequence
    val counts = data.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)

    counts.collect().foreach(word=>{
        if(word._2 != null) {
          writer.write(word._1 + ":" + word._2 + "\n")
          //println(word._1+":"+word._2 + "\n")

        }
    })

    writer.close()
  }

  def medicalwordcount(): Unit =
  {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setAppName("WordNetSpark").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)

    val data=sc.textFile("C:\\Users\\mraje\\Documents\\UMKC\\KDM\\source\\Retrive_abstract\\hay_fever\\medicalwords\\1_bp.txt");


    val writer = new PrintWriter(new File("C:\\Users\\mraje\\Documents\\UMKC\\KDM\\source\\Retrive_abstract\\hay_fever\\1_bpct.txt" ))

    val totalwords = data.flatMap(line => line.split(" ")).count()

    writer.write("total words " + totalwords + "\n")
    //word frequence
    val words = data.flatMap(line => line.split(" ")).map(word => (word,1))
    words.collect()
    val counts = words.reduceByKey(_+_)

    counts.collect().foreach(word=>{
      if(word._2 != null) {
        writer.write(word._1 + ":" + word._2 + "\n")
        println(word._1+":"+word._2 + "\n")

      }
    })

    writer.close()
  }


  def main(args: Array[String]): Unit = {


    //getWordNetStats()

    medicalwordcount()

    //print("word count " + wordcount())


    //val dd=data.map(line=>{
    //val synarr = CoreNLP.returnPos(line)

   /* val dd=data.map(abs=>{
      //println("lineeeeeeeeeeee " + line );


      val synarr = CoreNLP.returnPos(abs._2)
      synarr
    })*/


    //getPosStats()
    //getWordNetStats()

  }

  def getPosStats() : Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setAppName("WordNetSpark").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)


    //val data=sc.textFile("data/sample")
    //val data=sc.textFile("C:\\Users\\mraje\\Documents\\UMKC\\KDM\\source\\Retrive_abstract\\hay_fever\\abstractstxt\\1.txt");

    val data = sc.wholeTextFiles(path = "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\source\\Retrive_abstract\\hay_fever\\abstractstxt", minPartitions = 10)

    //val dd=data.map(line=>{
    //val synarr = CoreNLP.returnPos(line)

    val dd=data.map(abs=>{
      //println("lineeeeeeeeeeee " + line );
      val synarr = CoreNLP.returnPos(abs._2)
      synarr
    })

    val res = dd.flatMap(line=>line.split(",")).map(word=>(word,1))
    val output = res.reduceByKey(_+_)
    output.saveAsTextFile("Output_PosStats")

    val o=output.collect()

    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}

  }


  def getWordNetStats() : Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setAppName("WordNetSpark").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)


    //val data=sc.textFile("data/sample")
    val data=sc.textFile("C:\\Users\\mraje\\Documents\\UMKC\\KDM\\source\\Retrive_abstract\\hay_fever\\abstractstxt\\10.txt");

    val dd=data.map(line=>{
     // println("Wordnet " + line )

      val wordnet = new RiWordNet("C:\\Program Files (x86)\\WordNet\\2.1")
      val wordSet=line.split(" ")
      val synarr=wordSet.map(word=>{
        if(wordnet.exists(word))
          (word,1)
        else
        (word,0)
      })
      synarr
    })

    val flattenDD=dd.flatMap(f=>f)
    flattenDD.collect().foreach(f=>{
      //if (f._2 != 0)
        // println(f._1+":"+f._2)
    })

    val count = flattenDD.values.sum()
    println("words in wordnet " + count)
    val wc=flattenDD.reduceByKey(_+_)
    wc.collect().foreach(f=>{

      //println(f)
    })

/*
    println("f value = " + dd)

    //val output = dd.groupBy(identity).mapValues(_.length)

   //val output = dd.map(f=>f.flatten).map(word=>(word,1)).reduceByKey(_+_)
    val output = dd.
    output.saveAsTextFile("Output_WordNetStats")
    val o=output.collect()

    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}*/
  }

  def getSynoymns(wordnet:RiWordNet,word:String): Array[String] ={
    // println(word)
    val pos=wordnet.getPos(word)
    //println(pos.mkString(" "))
    val syn=wordnet.getAllSynonyms(word, pos(0), 10)
    syn
  }

}
