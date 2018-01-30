package com.app.core;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.languagetool.JLanguageTool;
import org.languagetool.language.AmericanEnglish;
import org.languagetool.rules.RuleMatch;

import edu.stanford.nlp.coref.statistical.DatasetBuilder;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.Pair;
import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.javatuples.Tuple;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Encoder;

import org.apache.spark.sql.functions.*;

public class Methods {

	static Properties props = new Properties();

	static {
		props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment");
	}
	static StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

	static JLanguageTool langTool = new JLanguageTool(new AmericanEnglish());

	public static String SpellCheck(String text) {

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

		} catch (IOException e) {
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

		SparkSession spark = SparkSession.builder().master("local[*]").appName("Sentiment Analyzer").getOrCreate();

		spark.sparkContext().hadoopConfiguration().get("mapreduce.input.fileinputformat.input.dir.recursive");
		spark.sparkContext().hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");

		Dataset<Row> data = spark.read()
				.json("/home/nikita/Documents/backupHDFS/day_key=20180119/FlumeData.1516317891260");

		/*
		 * data.createOrReplaceTempView("twitter");
		 * spark.sql("describe twitter").show();
		 */

		Encoder<Tuple2<String, Double>> encoder1 = Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE());

		Dataset<Tuple2<String, Double>> result = data.map(
				t -> Tuple2.apply(t.getAs("id").toString(), GetSentiment(SpellCheck(t.getAs("text").toString()))),
				encoder1);

		result.toDF("id", "Sentiment");

		data.join(result, data.col("id").equalTo(data.col("id"))).show();

	}

}
