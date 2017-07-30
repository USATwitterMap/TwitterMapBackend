package hadoopManager;
import java.io.IOException;
import java.io.StringReader;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;


/**
 * Hadoop Mapper class for consuming twitter data output
 * @author brett
 *
 */
public class TwitterDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * Takes twitter data as value and line number as key. Generates new key 
	 * for each state + word and a value of 1 for the counter
	 * 
	 * Example:
	 * Input:
	 * MN hello hi whats up
	 * 
	 * Output
	 * {MNhello, {1}}
	 * {MNhi, {1}}
	 * {MNwhats, {1}}
	 * {MNup, {1}}
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)

	throws IOException, InterruptedException 
	{
		 //get the state (always the first Token)
	    String state = value.toString().substring(0, 2);
	    String tweetContents = value.toString().substring(3);
		
		//set up Lucene Tokenizer to filter out stop words and tokenize;
		Analyzer analyzer = new StandardAnalyzer();
	    StringReader tReader = new StringReader(tweetContents);
		TokenStream tokenStream = analyzer.tokenStream("contents",tReader);
		CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
		tokenStream.reset();
	    
	    //add each token to Hadoop
	    while (tokenStream.incrementToken()) {
	    	
	    	String rawData = charTermAttribute.toString();
	    	CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();
	    	utf8Decoder.onMalformedInput(CodingErrorAction.REPLACE);
	    	utf8Decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
	    	utf8Decoder.replaceWith("?");
	    	
	    	CharsetEncoder encoder = Charset.forName("UTF-8").newEncoder();
	    	encoder.encode(CharBuffer.wrap(rawData));
	    	
	    	// removing non UTF-8 symbols (for SQL insertion later)
	    	CharBuffer cleanData = utf8Decoder.decode(encoder.encode(CharBuffer.wrap(rawData)));	    	
	    	context.write(new Text(state + cleanData.toString()), new IntWritable(1));
	    }
	    
	    tokenStream.close();
	    analyzer.close();
	}

}