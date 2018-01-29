//for spell check
import org.languagetool.JLanguageTool
import org.languagetool.language.AmericanEnglish
import scala.collection.JavaConversions._

//for sentiment analyzer
import java.util.Properties
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import scala.collection.JavaConversions._
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree

class Methods {
  
  // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
  val props = new Properties()
  props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment")
  val pipeline = new StanfordCoreNLP(props)
  
  // Language Tool 
  val langTool = new JLanguageTool(new AmericanEnglish())


  def SpellCheck(text: String): String = {

    val query = text

    val matches = langTool.check(query)
    var result = ""
    var lastPos = 0
    var tmp = ""

    for (ma <- matches) {
      try {

        tmp = ma.getSuggestedReplacements().get(0)
        result += query.substring(lastPos, ma.getFromPos())
        result += tmp
        lastPos = ma.getToPos()

      } catch {
        case e: Exception => println()
      }
    }

    if (lastPos < query.length()) {
      result += query.substring(lastPos, query.length())
    }

    result
  }

  
    def Sentiments(checkedText: String): Double = {
    
    // read some text in the text variable
    val text = checkedText
    
    // create an empty Annotation just with the given text
    val document = new Annotation(text)

    // run all Annotators on this text
    pipeline.annotate(document)

    val sentences = document.get(classOf[SentencesAnnotation])
    
    var sum = 0.0
    
    for (sentence <- sentences) {

      // Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class)
      val tree = sentence.get(classOf[SentimentAnnotatedTree])

      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val scaled = sentiment - 2
      
      sum = sum + scaled //doing -2 to bring neutral on 0
      //System.out.println("Sentiment Scale : scale of 0 = very negative, 1 = negative, 2 = neutral, 3 = positive and 4 = very positive.")
      //System.out.println("Statement : " + sentence + " == Sentiment : " + sentiment + " \n")
      println(sentence,scaled)
    }

    val total = sum / sentences.length
    
    
    total
  }
}
