import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class test {

	public static void main(String[] args) {
    	String text    = "<word,5>,<lemma,5>,<lemma,5>,<lemma,5>,<lemma,5>";
    	splitter(text);
	}
	
    public static void splitter(String text){


    	String patternString1 = "<(([^>]+),(\\d+))>";

    	Pattern pattern = Pattern.compile(patternString1);
    	Matcher matcher = pattern.matcher(text);

    	while(matcher.find()) {
    	    System.out.println("found: " + matcher.group(1).split(",")[1]);
    	}
    }

}
