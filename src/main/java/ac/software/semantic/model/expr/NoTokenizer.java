package ac.software.semantic.model.expr;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.AttributeFactory;

public final class NoTokenizer extends CharTokenizer {
	  
	  /**
	   * Construct a new WhitespaceTokenizer.
	   */
	  public NoTokenizer() {
	  }

	  /**
	   * Construct a new WhitespaceTokenizer using a given
	   * {@link org.apache.lucene.util.AttributeFactory}.
	   *
	   * @param factory
	   *          the attribute factory to use for this {@link Tokenizer}
	   */
	  public NoTokenizer(AttributeFactory factory) {
	    super(factory);
	  }

	  /**
	   * Construct a new WhitespaceTokenizer using a given max token length
	   *
	   * @param maxTokenLen maximum token length the tokenizer will emit.
	   *        Must be greater than 0 and less than MAX_TOKEN_LENGTH_LIMIT (1024*1024)
	   * @throws IllegalArgumentException if maxTokenLen is invalid.
	   */
	  public NoTokenizer(int maxTokenLen) {
	    super(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY, maxTokenLen);
	  }

	  /**
	   * Construct a new WhitespaceTokenizer using a given
	   * {@link org.apache.lucene.util.AttributeFactory}.
	   *
	   * @param factory the attribute factory to use for this {@link Tokenizer}
	   * @param maxTokenLen maximum token length the tokenizer will emit. 
	   *        Must be greater than 0 and less than MAX_TOKEN_LENGTH_LIMIT (1024*1024)
	   * @throws IllegalArgumentException if maxTokenLen is invalid.
	   */
	  public NoTokenizer(AttributeFactory factory, int maxTokenLen) {
	    super(factory, maxTokenLen);
	  }
	  
	  /** Collects only characters which do not satisfy
	   * {@link Character#isWhitespace(int)}.*/
	  @Override
	  protected boolean isTokenChar(int c) {
	    return true;
	  }
	}