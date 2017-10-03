package common.parser;

public class Constant<T extends Comparable> extends Term implements Parseable {

  T value;

  public Constant(T value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "const:'" + value + "'";
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Constant && value.equals(((Constant) obj).value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public int compareTo(Object o) {
    if (o instanceof Constant) {
      return value.compareTo(((Constant) o).value);
    } else {
      return toString().compareTo(o.toString()); //TODO: inefficient
    }
  }

  public T getValue() {
    return value;
  }
}
