package common.parser;

public class Constant extends Term implements Parseable {

  Object value;

  @Override
  public String toString() {
    return "const:'" + value + "'";
  }
}
