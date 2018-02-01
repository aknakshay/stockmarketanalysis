package com.app.core;

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
import org.apache.spark.api.java.function.MapFunction;
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

			System.out.print("Exception for " + e.getStackTrace());
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

		return total;
	}

	public static void main(String[] args) throws AnalysisException {
		
		if(args.length != 2) {
			System.out.println("enter only two arguements. Input and Output");
			System.exit(-1);
		}

		SparkSession spark = SparkSession.builder().appName("Sentiment Analyzer").master("local[*]").getOrCreate();

		//Setting recursive properties to read files from subdirectories as well
		spark.sparkContext().hadoopConfiguration().get("mapreduce.input.fileinputformat.input.dir.recursive");
		spark.sparkContext().hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");

		//input address
		Dataset<Row> data = spark.read().json(args[0]);

	
		
		//Ignore
		//We don't requre the encoders not, but incase we plan to use tuples, we might require them
		// Encoder<Tuple2<String, Double>> encoder1 = Encoders.tuple(Encoders.STRING(),
		
	
		// Register a UDF of name "Sentiment" to get Sentiment results after getting the text SpellChecked
		spark.sqlContext().udf().register("Sentiment", (String s) -> GetSentiment(SpellCheck(s)), DataTypes.DoubleType);
	
		
		//Create a temporary view named structure
		data.createOrReplaceTempView("structure");

		// Gets the sentiment values as well, along with desired format of timestamp and partitionBy field(date)
		Dataset<Row> withSentiments = spark.sql("select concat(substr(created_at,5,6), substr(created_at,26,5),' ',substr(created_at,12,6),'00') as timestamp,Sentiment(text) as Sentiments,* ,SUBSTR(created_at,5,6) as partitionBy from structure limit 100");
		
		//Creates a view named twitter
		withSentiments.createOrReplaceTempView("twitter");
		 
		
		//gets the net feeling. Net feeling formula currently is followers_count * sentiments but also try with
		//square root of followers_count * sentiments
		Dataset<Row> net = spark.sql("select *,lower(text) as main_text,user.followers_count * Sentiments as netSentiment from twitter");

		net.filter(col("main_text").contains("apple")).groupBy("timestamp","partitionBy").mean("netSentiment").
		map(
			    (MapFunction<Row, Double>) a -> a.getAs("avg(netSentiment)"),
			    Encoders.DOUBLE());
		
		List<String> list = Arrays.asList("apple", "google", "tesla", "infosys", "tcs", "oracle", "microsoft", "facebook");
/*		
		for(String company : list ) {
			String path = args[1] + company;
			//Get's the mean of netSentiment per minute for each company
			net.filter(col("main_text").contains(company)).groupBy("timestamp","partitionBy").mean("netSentiment").write().partitionBy("partitionBy").format("json").save(path);	
		}
*/
		
	}

}
