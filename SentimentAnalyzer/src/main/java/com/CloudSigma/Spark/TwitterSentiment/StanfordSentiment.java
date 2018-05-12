package com.CloudSigma.Spark.TwitterSentiment;

import java.util.List;
import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import static com.CloudSigma.Spark.TwitterSentiment.LanguageCheck.*;


public class StanfordSentiment  {

	static Properties props = new Properties();
	static {
	props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment");}
	static StanfordCoreNLP pipeline = new StanfordCoreNLP(props);


	public static Double GetSentiment(String text) {
		String checkedText = CorrectSpell(text);

		Annotation document = new Annotation(checkedText);
		pipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);

		Double sum = 0.0;

		for (CoreMap sentence : sentences) {
			Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
			int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
			int scaled = sentiment - 2;
			sum = sum + scaled;
		}

		Double total = sum / sentences.size();
		System.out.println("Tweet Text: " + checkedText);
		System.out.println("Sentiment Value: " + total);
		return total;
	}

}
