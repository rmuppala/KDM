


import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
//import TweetWithSentiment;
import edu.stanford.nlp.trees.Tree;



import edu.stanford.nlp.util.CoreMap;
public class SentimentAnalyzerJava{
    public int findSentiment(String line) {
    //public TweetWithSentiment findSentiment(String line) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
      StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
      int mainSentiment = 0;
        if (line != null && line.length() > 0) {

            int longest = 0;

            Annotation annotation = pipeline.process(line);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {

                //Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
                Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
           String partText = sentence.toString();
            if (partText.length() > longest) {
                     mainSentiment = sentiment;
                    longest = partText.length();
            }

            }

        }

        /*if (mainSentiment == 2 || mainSentiment > 4 || mainSentiment < 0) {
            return null;

        }*/



        //TweetWithSentiment tweetWithSentiment = new TweetWithSentiment(line, toCss(mainSentiment));



        return mainSentiment;



    }


    private String toCss(int sentiment) {


        switch (sentiment) {
            case 0:
                return "alert alert-danger";
            case 1:
                return "alert alert-danger";
            case 2:
                return "alert alert-warning";
            case 3:
                return "alert alert-success";
            case 4:
               return "alert alert-success";
            default:
                return "";
        }



    }

    public static void main(String[] args) {



        SentimentAnalyzerJava sentimentAnalyzer = new SentimentAnalyzerJava();



        /*TweetWithSentiment tweetWithSentiment = sentimentAnalyzer
                .findSentiment("click here for your Sachin Tendulkar personalized digital autograph.");*/


        int svalue = sentimentAnalyzer
                .findSentiment("is excited for the future and we are even more excited to expand our #Uncarrier strategy with @Sprint as New T-Mobile. We continued to dominate this quarter .");

        System.out.println("Tweet sentiment analysis " + svalue);

        int svalue1 = sentimentAnalyzer
                .findSentiment("I can’t stand not being able to hear my girlfriend over the phone because of service. I want to hear her voice because that’s all I have of her now.");

                        System.out.println("Tweet sentiment analysis " + svalue1);




    }



}
