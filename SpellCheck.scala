import org.languagetool.JLanguageTool
import org.languagetool.language.AmericanEnglish
import scala.collection.JavaConversions._

object SpellCheck extends App {
  
  val langTool = new JLanguageTool(new AmericanEnglish());

  val query = args(0)
  

    val matches = langTool.check(query);
    var result = "";
    var lastPos = 0;
    var tmp = ""

    for ( ma <- matches) {
      try{

        tmp = ma.getSuggestedReplacements().get(0);
        result += query.substring(lastPos, ma.getFromPos())
        result += tmp
        lastPos = ma.getToPos();     

      } catch{
        case e : Exception => println()
      }
    }

    if (lastPos < query.length()) {
        result += query.substring(lastPos, query.length());
    }

    print(result);

}




