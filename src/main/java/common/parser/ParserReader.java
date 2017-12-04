package common.parser;

import java.util.Arrays;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flinklog.FlinkEvaluator;
import javatools.administrative.CallStack;

/** Helper class for parsing. It provides convenience methods for reading characters from a string, for example names, literals (numbers, strings), and character sequences */
public class ParserReader {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkEvaluator.class);

  /** The complete string we parse */
  private String input;

  /** Index of the character that we read next */
  private int pos;

  /** Parse string "input" */
  public ParserReader(String input) {
    this.input = input;
  }

  /**
   * Skips comments and tries to find the expected string
   * @return the expected string if found, null otherwise
   */
  public String consume(String... toConsume) {
    debug();
    skipComments();
    debug();
    for (String consume : toConsume) {
      String toCheck = input.substring(pos, Math.min(input.length(), pos + consume.length()));
      if (consume.equals(toCheck)) {
        pos += consume.length();
        return consume;
      }
    }
    return null;
  }

  /** Like {@link #consume(String...)}, but prints an error message and throws an exception. */
  public String expect(String... expect) {
    String found = consume(expect);
    if (found != null) {
      return found;
    }
    String method = CallStack.toString(new CallStack().ret().top());

    // mark the position with "__>"
    int act = 0, line = 1;
    while (true) {
      int next = input.indexOf('\n', act);
      if (next < 0 || next > pos) break;
      line++;
      act = next + 1;
    }
    int until = input.indexOf('\n', pos);
    if (until < 0) until = input.length();

    String error = "error in " + method + ": expected \"" + (expect.length == 1 ? expect[0] : Arrays.toString(expect)) + "\"";
    error += " at line " + line + ": " + input.substring(act, pos) + "__>" + input.substring(pos, until);
    throw new ParseException(error);
  }

  /** Prints method and current position */
  public void debug() {
    if (LOG.isDebugEnabled()) {
      String method = CallStack.toString(new CallStack().ret().top());
      LOG.debug(method + ": " + input.substring(0, pos) + "__>" + input.substring(pos));
    }
  }

  /** Advance position to the next non-whitespace character */
  public void skipWhitespace() {
    debug();
    while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
      pos++;
    }
  }

  /** Look at next character, without changing position */
  public Character peek() {
    if (pos >= input.length()) return '\0';
    return input.charAt(pos);
  }

  /** Look at rest of line, without changing position */
  String peekLine() {
    if (pos >= input.length()) return null;
    int idx = input.indexOf("\n", pos);
    if (idx < 0) idx = input.length();
    return input.substring(pos, idx);
  }

  /** Read one character, and update position */
  public Character read() {
    if (pos >= input.length()) return null;
    return input.charAt(pos++);
  }

  /** Read until next linebreak, and update position */
  String readLine() {
    return readWhile((c, s) -> c != '\n');
  }

  /** Read characters while fn returns true. The StringBuilder contains the accepted characters. **/
  public String readWhile(BiFunction<Character, StringBuilder, Boolean> fn) {
    StringBuilder sb = new StringBuilder();
    Character c;
    while ((c = peek()) != '\0') {
      if (fn.apply(c, sb)) {
        sb.append(read());
      } else {
        break;
      }
    }
    debug();
    String r = sb.toString();
    if (r.length() == 0) return null;
    return sb.toString();
  }

  /** Read a string. The first character  */
  public String readString() {
    debug();
    char s = read();
    if (s != '"' && s != '\'') {
      throw new ParseException("string should start with single quote ' or double quote \"");
    }
    boolean escaped = false;
    StringBuilder sb = new StringBuilder();
    Character c;
    while ((c = read()) != null) {
      if (escaped) {
        escaped = false;
        sb.append(c);
      } else if (c == '\\') {
        escaped = true;
      } else if (c == s) {
        if (peek() == c) {
          sb.append(c);
          read();
          continue;
        }
        break;
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /** Read a name consisting of letters, digits, and underscores */
  public String readName() {
    debug();
    skipComments();

    return readWhile((c, sb) -> Character.isAlphabetic(c) || c == '_' || (sb.length() > 0 && Character.isDigit(c)));
  }

  /** While next character is a '%', advance to first non-space character in next line. */
  public void skipComments() {
    do {
      while (peek() != null && peek() == '%') {
        readWhile((c, sb) -> c != '\n');
      }
      skipWhitespace();
    } while (peek() != null && peek() == '%');
  }

  /** Read an integer or floating number. */
  public Number readNumber() {
    int i = 0;
    while (Character.isDigit(peek())) {
      i *= 10;
      i += read() - '0';
    }
    if (peek() == '.') {
      Double d = (double) i;
      int div = 1;
      int r = 0;
      while (Character.isDigit(peek())) {
        div *= 10;
        r *= 10;
        r += read() - '0';
      }
      d += ((double) r) / div;
      return d;
    }
    return i;
  }

}
