package common.parser;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public abstract class Term implements Parseable, Comparable<Object> {

  public static Term read(ParserReader pr, Map<String, Variable> varMap, Set<String> supportedFeatures) {
    pr.debug();
    pr.skipComments();
    if (pr.peek() == null) return null;

    if (pr.peek() == '_' || Character.isUpperCase(pr.peek())) {
      return Variable.read(pr, varMap, supportedFeatures);
    }

    if (pr.peek() == '[') {
      return TermList.read(pr, varMap, supportedFeatures);
    }

    if (pr.peek() == '"' || pr.peek() == '\'') {
      return new Constant<>(pr.readString());
    }

    if (Character.isDigit(pr.peek())) {
      return new Constant<Comparable<?>>((Comparable<?>) pr.readNumber());
    }

    if (Character.isLowerCase(pr.peek())) {
      if(!supportedFeatures.contains(Parseable.ATOMS)) {
        pr.error("variables need to start with an uppercase character", null);
      }
      Atom a = new Atom();
      a.name = pr.readName();

      pr.skipComments();
      if (pr.peek() == '(') {
        return CompoundTerm.read(a.name, false, pr, varMap, supportedFeatures);
      }

      return a;
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
