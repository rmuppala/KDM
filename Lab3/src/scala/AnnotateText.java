import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

public class AnnotateText {

    static final String REST_URL = "http://data.bioontology.org";
    static final String API_KEY = "4b1f5060-3d6a-433e-ac65-5b3f0cfc5fc8";
    static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String textToAnnotate = URLEncoder.encode("Melanoma is a malignant tumor of melanocytes which are found predominantly in skin but also in the bowel and the eye.", "ISO-8859-1");

        String medwdStr = getMedWords(textToAnnotate);
        String[] medwords = medwdStr.split(",");
        for (int i=0 ; i < medwords.length; i++) {
            String word= medwords[i];
            System.out.println(word);
        }
        //printMedWords(textToAnnotate);
    }

    public static String getMedWords(String line) throws Exception {

        String urlParameters;

        JsonNode annotations;
        //line = URLEncoder.encode("Melanoma is a malignant tumor of melanocytes which are found predominantly in skin but also in the bowel and the eye.", "ISO-8859-1");

       // line = URLEncoder.encode("Melanoma", "ISO-8859-1");
        //System.out.println(line);
        String textToAnnotate = URLEncoder.encode(line);
        StringBuffer words = new StringBuffer();
        StringBuffer ontology = new StringBuffer();
        // Get just annotations

        urlParameters = "text=" + textToAnnotate;
        System.out.println("Get Med Words 2" +   REST_URL + "/annotator?" + urlParameters);
        annotations = jsonToNode(get(REST_URL + "/annotator?" + urlParameters));

        for (JsonNode annotation : annotations) {

            try {
                JsonNode classDetails = jsonToNode(get(annotation.get("annotatedClass").get("links").get("self").asText()));
                if (classDetails != null) {
                    words.append(classDetails.get("prefLabel").asText() + ",");

                    System.out.println("\tontology: " + classDetails.get("links").get("ontology").asText());
                    ontology.append(classDetails.get("links").get("ontology").asText() + "," );
                }

            }
            catch( Exception ex)
            {

            }
        }
        System.out.println("med words " + words.toString() + "|" + ontology.toString());
        return words.toString() + "|" + ontology.toString() ;
    }

    public static String getOntology(String line) throws Exception {

        String urlParameters;
        JsonNode annotations;

        //line = URLEncoder.encode("Melanoma is a malignant tumor of melanocytes which are found predominantly in skin but also in the bowel and the eye.", "ISO-8859-1");

        // line = URLEncoder.encode("Melanoma", "ISO-8859-1");
        //System.out.println(line);
        String textToAnnotate = URLEncoder.encode(line);
        StringBuffer ontology = new StringBuffer();
        // Get just annotations

        urlParameters = "text=" + textToAnnotate;
        System.out.println("Get Med Words 2" +   REST_URL + "/annotator?" + urlParameters);
        annotations = jsonToNode(get(REST_URL + "/annotator?" + urlParameters));

        for (JsonNode annotation : annotations) {

            try {
                JsonNode classDetails = jsonToNode(get(annotation.get("annotatedClass").get("links").get("self").asText()));
                if (classDetails != null)
                    ontology.append(classDetails.get("ontology").asText() + "," );


            }
            catch( Exception ex)
            {

            }
        }
        System.out.println("med words " + ontology.toString());
        return ontology.toString();
    }

    private static JsonNode jsonToNode(String json) {
        JsonNode root = null;
        try {
            //System.out.println("Jsontonode " +json );
            root = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return root;
    }
    public static void printMedWords(String line) throws Exception {
        String urlParameters;
        JsonNode annotations;
        //String textToAnnotate = URLEncoder.encode("Melanoma is a malignant tumor of melanocytes which are found predominantly in skin but also in the bowel and the eye.", "ISO-8859-1");

        System.out.println("Medical line" + line);
        String textToAnnotate = URLEncoder.encode(line);

        // Get just annotations
        urlParameters = "text=" + textToAnnotate;
        annotations = jsonToNode(get(REST_URL + "/annotator?" + urlParameters));
        printAnnotations(annotations);

        // Annotations with hierarchy
        urlParameters = "max_level=3&text=" + textToAnnotate;
        annotations = jsonToNode(get(REST_URL + "/annotator?" + urlParameters));
        printAnnotations(annotations);

        // Annotations using POST (necessary for long text)
        urlParameters = "text=" + textToAnnotate;
        annotations = jsonToNode(post(REST_URL + "/annotator", urlParameters));
        printAnnotations(annotations);

        // Get labels, synonyms, and definitions with returned annotations
        urlParameters = "include=prefLabel,synonym,definition&text=" + textToAnnotate;
        annotations = jsonToNode(get(REST_URL + "/annotator?" + urlParameters));
        for (JsonNode annotation : annotations) {
            System.out.println(annotation.get("annotatedClass").get("prefLabel").asText());
        }
    }
    private static void printAnnotations(JsonNode annotations) {

        for (JsonNode annotation : annotations) {
            // Get the details for the class that was found in the annotation and print
            JsonNode classDetails = jsonToNode(get(annotation.get("annotatedClass").get("links").get("self").asText()));
            System.out.println("Class details");
            System.out.println("\tid: " + classDetails.get("@id").asText());
            System.out.println("\tprefLabel: " + classDetails.get("prefLabel").asText());
            System.out.println("\tontology: " + classDetails.get("links").get("ontology").asText());
            System.out.println("\n");

            JsonNode hierarchy = annotation.get("hierarchy");
            // If we have hierarchy annotations, print the related class information as well
            if (hierarchy.isArray() && hierarchy.elements().hasNext()) {
                System.out.println("\tHierarchy annotations");
                for (JsonNode hierarchyAnnotation : hierarchy) {
                    classDetails = jsonToNode(get(hierarchyAnnotation.get("annotatedClass").get("links").get("self").asText()));
                    System.out.println("\t\tClass details");
                    System.out.println("\t\t\tid: " + classDetails.get("@id").asText());
                    System.out.println("\t\t\tprefLabel: " + classDetails.get("prefLabel").asText());
                    System.out.println("\t\t\tontology: " + classDetails.get("links").get("ontology").asText());
                }
            }
        }
    }



    private static String get(String urlToGet) {
        URL url;
        HttpURLConnection conn;
        BufferedReader rd;
        String line;
        String result = "";
        try {
            url = new URL(urlToGet);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "apikey token=" + API_KEY);
            conn.setRequestProperty("Accept", "application/json");
            rd = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            while ((line = rd.readLine()) != null) {
                result += line;
            }
            rd.close();
        } catch (Exception e) {
            //e.printStackTrace();
        }

        return result;
    }

    private static String post(String urlToGet, String urlParameters) {
        URL url;
        HttpURLConnection conn;

        String line;
        String result = "";
        try {
            url = new URL(urlToGet);
            conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "apikey token=" + API_KEY);
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("charset", "utf-8");
            conn.setUseCaches(false);

            DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
            wr.writeBytes(urlParameters);
            wr.flush();
            wr.close();
            conn.disconnect();

            BufferedReader rd = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            while ((line = rd.readLine()) != null) {
                result += line;
            }
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

}
