package common.parser;

import java.util.Map;
import java.util.stream.Stream;

public abstract class Term implements Parseable, Comparable {

  public static Term read(ParserReader pr, Map<String, Variable> varMap) {
    pr.debug();
    pr.skipComments();
    if (pr.peek() == null) return null;

    if (Character.isLowerCase(pr.peek())) {
      Atom a = new Atom();
      a.name = pr.readName();
      return a;
    }

    if (pr.peek() == '_' || Character.isUpperCase(pr.peek())) {
      return Variable.read(pr, varMap);
    }

    if (pr.peek() == '[') {
      return TermList.read(pr, varMap);
    }

    if (pr.peek() == '"' || pr.peek() == '\'') {
      return new Constant(pr.readString());
    }

    if (Character.isDigit(pr.peek())) {
      return new Constant(pr.readNumber());
    }

    return null;
  }

  public Stream<Variable> getVariables() {
    return Stream.empty();
  }

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);
}
