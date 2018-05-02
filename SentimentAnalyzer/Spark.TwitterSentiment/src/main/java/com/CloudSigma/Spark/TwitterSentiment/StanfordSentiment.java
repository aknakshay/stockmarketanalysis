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

public class StanfordSentiment {
	
	private StanfordCoreNLP pipeline;
	private LanguageCheck lc;

	public StanfordSentiment() {
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment");
		pipeline = new StanfordCoreNLP(props);
		lc = new LanguageCheck();
	}

	public Double GetSentiment(String text) {
		String checkedText = lc.CorrectSpell(text);

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
