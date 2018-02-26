package org.cdac.sentimentAnalyzer2;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.languagetool.JLanguageTool;
import org.languagetool.language.AmericanEnglish;
import org.languagetool.rules.RuleMatch;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.StanfordCoreNLPClient;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Methods {

    static Properties props = new Properties();

    static {
        props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment");
    }

    // Creating a StanfordCoreNLP object with minimum annotators required to do Sentiment Analysis
    static StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

    // Creating a JLanguageTool on with AmericanEnglish as the language to do SpellCheck on
    static JLanguageTool langTool = new JLanguageTool(new AmericanEnglish());


    public static String SpellCheck(String text) {

    /*
    Spell Check Method

    input: text having single or multiple sentences
    output: text having single or multiple sentences with corrected spellings
    
    Caution: It may change some of the proper nouns but that generally didn't affect the sentiment analysis in the sample testing that we did

    */

        String query = text;

        try {

            /*
            JLanguageTool's check method returns the list of RuleMatch objects which have various methods like getSuggestedReplacements to return the suitable replacements, getFromPos, getToPost to return the position of the incorrect words in the sentence
            */

            List<RuleMatch> matches = langTool.check(query);

            // We will be storing the final text in the result
            String result = "";

            int lastPos = 0;
            String tmp = "";

            for (RuleMatch ma : matches) {

                try {

                    tmp = ma.getSuggestedReplacements().get(0);
                    result += query.substring(lastPos, ma.getFromPos());
                    result += tmp;
                    lastPos = ma.getToPos();

                } catch (Exception e) {
                    return text;
                }
            }

            if (lastPos < query.length()) {
                result += query.substring(lastPos, query.length());
            }

            return result;

        } catch (Exception e) {
            return text;
        }

    }



    public static Double GetSentiment(String checkedText) {
    /*
    Get Sentiment Method
    
    input: text having single or multiple sentences which are spell checked
    output: double value of Sentiment of the entire tweet

    Sentiment Scale (Improvised):
    -2 : Very Negative
    -1 : Negative
     0 : Neutral
    +1 : Positive
    +2 : Very Positive
    
    Caution: We are assuming each sentence of the tweet has equal value since the tweets are posted by various different individual and each of them has a different writing style. 

    */
    
        String text = checkedText;

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(text);

        // Runs all Annotators on this text
        pipeline.annotate(document);

        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        Double sum = 0.0;

        for (CoreMap sentence : sentences) {

            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);

            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);

            // We are scaling the sentiment values bringing the netural from 2 to 0
            int scaled = sentiment - 2;

            sum = sum + scaled;

        }

        // Averaging the values
        Double total = sum / sentences.size();

        // To see in stdout the tweets and the values
        System.out.println("Checking for tweet : " + checkedText);
        System.out.println("Sentiment Value : " + total);


        return total;
    }



    public static void main(String[] args) throws AnalysisException {

        if(args.length != 2) {
            System.out.println("enter only two arguements. Input and Output");
            System.exit(-1);
        }

        // To test it on local, uncomment the one beneath and comment the one next
        //SparkSession spark = SparkSession.builder().appName("Sentiment Analyzer").master("local[*]").getOrCreate();

        SparkSession spark = SparkSession.builder().appName("Sentiment Analyzer").getOrCreate();

        //Setting recursive properties to read files from subdirectories as well
        spark.sparkContext().hadoopConfiguration().get("mapreduce.input.fileinputformat.input.dir.recursive");
        spark.sparkContext().hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");

        // To Complete the input address as in my case, we have the data 1 level down
        String path = args[0] + "/*/*";

        // Reading the entire data
        Dataset<Row> data = spark.read().json(path);

        
        // Register a UDF of name "Sentiment" to get Sentiment results after getting the text SpellChecked
        spark.sqlContext().udf().register("Sentiment", (String s) -> GetSentiment(SpellCheck(s)), DataTypes.DoubleType);

        //List of companies to output data for
        List<String> list = Arrays.asList("apple", "google", "tesla", "infosys", "tcs", "oracle", "microsoft", "facebook");

        String query;
        String oPath;

        Dataset<Row> result;

        for(String company : list ) {

            //Output path address
            oPath = args[1] + "/" + company;

            //Create a temporary view named complete
            data.createOrReplaceTempView("complete");

            
            // tmp1 extracts TimeStamp, partitionBy (Date), tweet text, tweet text in lower case and followers_count of the user tweeting 
    		Dataset<Row> tmp1 = spark.sql("select concat(substr(created_at,5,6), substr(created_at,26,5),' ',substr(created_at,12,6),'00') as timestamp,substr(created_at,5,6) as partitionBy,text,lower(text) as main_text,user.followers_count as followers from complete");


            tmp1.createOrReplaceTempView("tmp");

            //Filtering tweets having certain company names in it
            Dataset<Row> tmp2 = spark.sql("select * from tmp where main_text regexp '(" + company + ")'");
            
            //Creates a view named twitter
            tmp2.createOrReplaceTempView("twitter");

            
            // tmp3 contains the entire selected data along with the Sentiment value of the tweets
            Dataset<Row> tmp3 = spark.sql("select  *, Sentiment(text) as seVal from twitter");

            /* We are persisting the serialized data in memory and disk as we want the entire result to be stored, as sentiment analysis is a computational heavy task.
            If we don't persist this, in the next query where we are using Sentiment method's value 10 times, it would be doing Sentiment Analysis of the same tweet 10 times.
            */
            tmp3.persist(StorageLevel.MEMORY_AND_DISK_SER());
            
            // Creating another view 
            tmp3.createOrReplaceTempView("dataSe");

            Dataset<Row> net = spark.sql("select  *,POWER(followers,1)*seVal as 1Sentiment, POWER(followers,1/2)*seVal as 2Sentiment, POWER(followers,1/3)*seVal as 3Sentiment,POWER(followers,1/4)*seVal as 4Sentiment,POWER(followers,1/5)*seVal as 5Sentiment,POWER(followers,1/6)*seVal as 6Sentiment,POWER(followers,1/7)*seVal as 7Sentiment,POWER(followers,1/8)*seVal as 8Sentiment,POWER(followers,1/9)*seVal as 9Sentiment,POWER(followers,1/10)*seVal as 10Sentiment from dataSe");

            //Creating a final view to save the data
            net.createOrReplaceTempView("final");

            // Averaging the Sentiment Values per minute by grouping the data onto it
            query = "select timestamp,partitionBy,AVG(1Sentiment),AVG(2Sentiment),AVG(3Sentiment),AVG(4Sentiment),AVG(5Sentiment),AVG(6Sentiment),AVG(7Sentiment),AVG(8Sentiment),AVG(9Sentiment),AVG(10Sentiment) from final group by timestamp,partitionBy";

            // Saving the result in result dataset
            result = spark.sql(query);

            // Writing the result onto the disk
            result.write().partitionBy("partitionBy").json(oPath);

        }


    }

}