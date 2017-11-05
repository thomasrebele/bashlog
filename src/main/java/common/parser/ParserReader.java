package common.parser;

import java.util.Arrays;
import java.util.function.BiFunction;

import javatools.administrative.CallStack;

public class ParserReader {

  private String input;

  private int pos;

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

  public String expect(String... expect) {
    String found = consume(expect);
    if (found != null) {
      return found;
    }
    String method = CallStack.toString(new CallStack().ret().top());

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

  public void debug() {
    //String method = CallStack.toString(new CallStack().ret().top());
    //System.out.println(method + ": " + input.substring(0, pos) + "__>" + input.substring(pos));
  }

  public void skipWhitespace() {
    debug();
    while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
      pos++;
    }
  }

  public Character peek() {
    if (pos >= input.length()) return '\0';
    return input.charAt(pos);
  }

  String peekLine() {
    if (pos >= input.length()) return null;
    int idx = input.indexOf("\n", pos);
    if (idx < 0) idx = input.length();
    return input.substring(pos, idx);
  }

  public Character read() {
    if (pos >= input.length()) return null;
    return input.charAt(pos++);
  }

  String readLine() {
    return readWhile((c, s) -> c != '\n');
  }

  /** Read characters while fn returns true. The StringBuilder contains the accepted characters. */
  public String readWhile(BiFunction<Character, StringBuilder, Boolean> fn) {
    StringBuilder sb = new StringBuilder();
    Character c;
    while ((c = peek()) != null) {
      // TODO: don't start with numbers
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

  public String readString() {
    debug();
    char s = read();
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

  public String readName() {
    debug();
    skipComments();

    // TODO: don't start with numbers
    return readWhile((c, sb) -> Character.isAlphabetic(c) || c == '_' || Character.isDigit(c));
  }

  public void skipComments() {
    do {
      while (peek() != null && peek() == '%') {
        readWhile((c, sb) -> c != '\n');
      }
      skipWhitespace();
    } while (peek() != null && peek() == '%');
  }

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
