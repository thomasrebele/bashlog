package common.parser;

import java.util.Map;

public class Term implements Parseable {

  public static Term read(ParserReader pr, Map<String, Variable> varMap) {
    pr.debug();
    pr.skipComments();
    if (pr.peek() == null) return null;

    if (Character.isLowerCase(pr.peek())) {
      Atom a = new Atom();
      a.name = pr.readName();
      return a;
    }

    if (pr.peek() == '_' | Character.isUpperCase(pr.peek())) {
      return Variable.read(pr, varMap);
    }

    if (pr.peek() == '[') {
      return TermList.read(pr, varMap);
    }

    if (pr.peek() == '"' || pr.peek() == '\'') {
      Constant c = new Constant();
      c.value = pr.readString();
      return c;
    }

    if (Character.isDigit(pr.peek())) {
      Constant c = new Constant();
      c.value = pr.readNumber();
      return c;
    }

    return null;
  }

}
