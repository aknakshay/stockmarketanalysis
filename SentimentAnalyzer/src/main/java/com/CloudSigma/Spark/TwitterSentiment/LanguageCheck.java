package com.CloudSigma.Spark.TwitterSentiment;

import java.util.List;
import org.languagetool.JLanguageTool;
import org.languagetool.language.AmericanEnglish;
import org.languagetool.rules.RuleMatch;


public class LanguageCheck {
	
	static JLanguageTool langTool = (new JLanguageTool(new AmericanEnglish()));

    public static String CorrectSpell(String text) {
 	
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
    
}
