package common.parser;

import java.util.Map;
import java.util.stream.Stream;

public class Variable extends Term implements Parseable {

  public final String name;

  public final int index;

  public Variable(String name, int index) {
    this.name = name;
    this.index = index;
  }

  @Override
  public String toString() {
    return "var:" + name + "@" + ("" + System.identityHashCode(this)).substring(0, 3);
  }

  public static Variable read(ParserReader pr, Map<String, Variable> varMap) {
    pr.debug();
    pr.skipComments();
    if (pr.peek() == null) return null;
    String name = null;
    name = pr.readName();
    if ("_".equals(name)) {
      name = "var_" + varMap.size();
    }
    if (name != null) {
      Variable v = varMap.computeIfAbsent(name, k -> {
        return new Variable(k, varMap.size());
      });
      return v;
    }
    return null;
  }

  @Override
  public Stream<Variable> getVariables() {
    return Stream.of(this);
  }
}
