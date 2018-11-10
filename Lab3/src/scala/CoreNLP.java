import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import edu.stanford.nlp.util.Quadruple;

import java.util.Collection;
import java.util.List;

/**
 * Created by Mayanka on 27-Jun-16.
 */
public class CoreNLP {
    public static String returnTriplets(String sentence) {

        Document doc = new Document(sentence);
        String lemma="";
        for (Sentence sent : doc.sentences()) {  // Will iterate over two sentences


            Collection<Quadruple<String, String, String, Double>> l=sent.openie();
            for (int i = 0; i < l.toArray().length ; i++) {
                lemma+= l.toString();
            }
            System.out.println(lemma);
        }

        return lemma;
    }

    public static String returnPos(String sentence) {

        Document doc = new Document(sentence);
        String pos="";
        List<String> l = null;
        StringBuffer posVal= new StringBuffer();
        for (Sentence sent : doc.sentences()) {  // Will iterate over two sentences
            //System.out.println("sentence " + sent);
             l=sent.posTags();


        }
        for (int i=0; i<l.size();i++) {
            posVal.append( l.get(i) + ",");
        }
        //System.out.println("POS VAL " + posVal.toString());
        return posVal.toString();
    }

    public static String returnVerb(String sentence) {

        Document doc = new Document(sentence);
        StringBuffer verbstr = new StringBuffer();
        for (Sentence sent : doc.sentences()) {

            //System.out.println("The parse of the sentence '" + sent + "' is " + sent.parse());
            //Iterate over every word in the sentence
            for(int i = 0; i < sent.words().size(); i++) {

                //Condition: if the word is a noun (posTag starts with "NN")
                if (sent.posTag(i) != null && sent.posTag(i).contains("NN")) {
                    //Put the word into the Set
                    //System.out.println(("noun = " +sent.word(i) ));
                    verbstr.append(sent.word(i) + ",");
                }
            }
        }

        //System.out.println("POS VAL " + posVal.toString());
        return verbstr.toString();
    }

    public static String returnNer(String sentence) {

        Document doc = new Document(sentence);
        StringBuffer nerstr = new StringBuffer();
        for (Sentence sent : doc.sentences()) {

            ;
            //System.out.println("The parse of the sentence '" + sent + "' is " + sent.parse());
            //Iterate over every word in the sentence
            for(int i = 0; i <sent.nerTags().size(); i++) {

                //Condition: if the word is a noun (posTag starts with "NN")
                if (sent.nerTag(i) != null && (!sent.nerTag(i).equals("O")) ) {
                    //Put the word into the Set
                    //System.out.println(("noun = " +sent.word(i) ));
                    nerstr.append(sent.word(i) + ":" + sent.nerTag(i) + ",");
                }
            }
        }

        //System.out.println("POS VAL " + posVal.toString());
        return nerstr.toString();
    }

    public static String returnNoun(String sentence) {

        Document doc = new Document(sentence);
        StringBuffer nounstr = new StringBuffer();
        for (Sentence sent : doc.sentences()) {

            //System.out.println("The parse of the sentence '" + sent + "' is " + sent.parse());
            //Iterate over every word in the sentence
            for(int i = 0; i < sent.words().size(); i++) {
                //Condition: if the word is a noun (posTag starts with "NN")
                if (sent.posTag(i) != null && sent.posTag(i).contains("NN")) {
                    //Put the word into the Set
                    //System.out.println(("noun = " +sent.word(i) ));
                    nounstr.append(sent.word(i) + ",");
                }
            }
        }

        //System.out.println("POS VAL " + posVal.toString());
        return nounstr.toString();
    }


    public static void main(String args[])
    {
        String sentence = "This book is on table";
        //returnTriplets(sentence);
        returnPos(sentence);
    }
}
