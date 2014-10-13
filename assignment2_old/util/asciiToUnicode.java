//package util;

import java.net.URLDecoder;
import java.io.UnsupportedEncodingException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;


/**
 * This class converts character in ASCII to one in unicode.
 * Example: Vladim%C3%ADr Leitner -> Vladimdr Leitner (with a little dash over the A)
 * @author edenzik
 * @since Oct 9, 2014
 */
public class asciiToUnicode {
	
	/** Main class is used for unit testing only
	 * @param args may be used to pass an ascii character
	 * @return unicode character
	 */
	public static void main(String[] args) throws UnsupportedEncodingException, IOException{
		if (args.length > 0) {
			if (args[0].equalsIgnoreCase("read")){
				BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
				String s;
				while ((s = input.readLine()) != null){
					System.out.println(parse(s));
				}
			}
		}
	}

	public static String parse(String line) throws UnsupportedEncodingException{
		return URLDecoder.decode(line, "UTF-8");
	}
	

	
}