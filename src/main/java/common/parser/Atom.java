package common.parser;

public class Atom extends Term implements Parseable {

  String name;

  @Override
  public String toString() {
    return "atom:" + name;
  }

  @Override
  public int compareTo(Object o) {
    if (o instanceof Atom) {
      return name.compareTo(((Atom) o).name);
    } else {
      return toString().compareTo(o.toString()); //TODO: inefficient
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Atom && name.equals(((Atom) obj).name);
    }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
