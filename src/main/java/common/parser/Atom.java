package common.parser;

public class Atom extends Term implements Parseable {

  String name;

  @Override
  public String toString() {
    return "atom:" + name;
  }
}
