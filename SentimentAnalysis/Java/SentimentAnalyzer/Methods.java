package org.cdac.sentimentAnalyzer2;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.lang.Math;

import org.languagetool.JLanguageTool;
import org.languagetool.language.AmericanEnglish;
import org.languagetool.rules.RuleMatch;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

public class Methods {

	static Properties props = new Properties();

	static {
		props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment");
	}
	static StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

	static JLanguageTool langTool = new JLanguageTool(new AmericanEnglish());

	public static String SpellCheck(String text) {

		// String clean = text.replaceAll("\\P{Print}", "");

		String query = text;

		try {
			List<RuleMatch> matches = langTool.check(query);

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

		// creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER,
		// parsing, and coreference resolution
		/*
		 * val props = new Properties() props.setProperty("annotators",
		 * "tokenize,ssplit,pos,parse,sentiment") val pipeline = new
		 * StanfordCoreNLP(props)
		 */

		// read some text in the text variable
		String text = checkedText;
		// create an empty Annotation just with the given text
		Annotation document = new Annotation(text);

		// run all Annotators on this text
		pipeline.annotate(document);

		List<CoreMap> sentences = document.get(SentencesAnnotation.class);

		Double sum = 0.0;

		for (CoreMap sentence : sentences) {

			// Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class)
			Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);

			int sentiment = RNNCoreAnnotations.getPredictedClass(tree);

			int scaled = sentiment - 2;

			sum = sum + scaled; // doing -2 to bring neutral on 0
			// System.out.println("Sentiment Scale : scale of 0 = very negative, 1 =
			// negative, 2 = neutral, 3 = positive and 4 = very positive.")
			// System.out.println("Statement : " + sentence + " == Sentiment : " + sentiment
			// + " \n")

		}

		Double total = sum / sentences.size();

		System.out.println("Checking for tweet : " + checkedText);
		System.out.println("Sentiment Value : " + total);
		return total;
	}

	public static void main(String[] args) throws AnalysisException {
		
		if(args.length != 2) {
			System.out.println("enter only two arguements. Input and Output");
			System.exit(-1);
		}

		 //SparkSession spark = SparkSession.builder().appName("Sentiment Analyzer").master("local[*]").getOrCreate();
		SparkSession spark = SparkSession.builder().appName("Sentiment Analyzer").getOrCreate();

		//Setting recursive properties to read files from subdirectories as well
		spark.sparkContext().hadoopConfiguration().get("mapreduce.input.fileinputformat.input.dir.recursive");
		spark.sparkContext().hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");

		//input address 
		String path = args[0] + "/*/*";
		
		Dataset<Row> data = spark.read().json(path);
	
		
		//Ignore
		//We don't requre the encoders not, but incase we plan to use tuples, we might require them
		// Encoder<Tuple2<String, Double>> encoder1 = Encoders.tuple(Encoders.STRING(),
		
	
		// Register a UDF of name "Sentiment" to get Sentiment results after getting the text SpellChecked
		spark.sqlContext().udf().register("Sentiment", (String s) -> GetSentiment(SpellCheck(s)), DataTypes.DoubleType);
	
		
		//Create a temporary view named structure
		data.createOrReplaceTempView("structure");

		// Gets the sentiment values as well, along with desired format of timestamp and partitionBy field(date)
		Dataset<Row> withSentiments = spark.sql("select concat(substr(created_at,5,6), substr(created_at,26,5),' ',substr(created_at,12,6),'00') as timestamp,Sentiment(text) as Sentiments,* ,SUBSTR(created_at,5,6) as partitionBy from structure");
		
		//Creates a view named twitter
		withSentiments.createOrReplaceTempView("twitter");
		 
		
		//gets the net feeling. Net feeling formula currently is followers_count * sentiments but also try with
		//square root of followers_count * sentiments
		Dataset<Row> net = spark.sql("select *,lower(text) as main_text,SQRT(user.followers_count) * Sentiments as netSentiment from twitter");


		//Creating a final view to save the data
		net.createOrReplaceTempView("final");
		
		//List of companies to output data for
		List<String> list = Arrays.asList("apple", "google", "tesla", "infosys", "tcs", "oracle", "microsoft", "facebook");

		String query;
		String oPath;
		Dataset<Row> result;
		//Filter and save the data
		for(String company : list ) {
			oPath = args[1] + "/" + company;
			
			query = "select timestamp,AVG(netSentiment) as feeling from final where main_text regexp '(" + company + ")' group by timestamp";
			result = spark.sql(query);
			//result.write().format("json").save(oPath);
			result.write().json(oPath);

		}

		
	}

}
