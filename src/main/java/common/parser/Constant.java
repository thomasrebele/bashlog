package common.parser;

public class Constant extends Term implements Parseable {

  public Object value;

  @Override
  public String toString() {
    return "const:'" + value + "'";
  }
}
