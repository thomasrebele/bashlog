package common.parser;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class Variable extends Term implements Parseable {

  public final String name;

  public Variable(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }

  public static Variable read(ParserReader pr, Map<String, Variable> varMap, Set<String> supportedFeatures) {
    pr.debug();
    pr.skipComments();
    if (pr.peek() == null) return null;
    String name = pr.readName();
    if ("_".equals(name)) {
      name = "Var_" + varMap.size();
    }
    if (name != null) {
      return varMap.computeIfAbsent(name, Variable::new);
    }
    return null;
  }

  @Override
  public Stream<Variable> getVariables() {
    return Stream.of(this);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Variable && name.equals(((Variable) obj).name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public int compareTo(Object o) {
    if (o instanceof Variable) {
      return name.compareTo(((Variable) o).name);
    } else {
      return toString().compareTo(o.toString()); //TODO: inefficient
    }
  }
}
