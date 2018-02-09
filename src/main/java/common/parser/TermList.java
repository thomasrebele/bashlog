package common.parser;

import java.util.*;
import java.util.stream.Stream;

public class TermList extends Term {

  private List<Term> terms = new ArrayList<>();

  public TermList() {

  }

  public TermList(Term... terms) {
    this.terms = Arrays.asList(terms);
  }

  public static TermList read(ParserReader pr, Map<String, Variable> varMap, Set<String> supportedFeatures) {
    if (pr.consume("[") != null) {
      TermList tl = new TermList();
      while (pr.peek() != ']') {
        Term t = Term.read(pr, varMap, supportedFeatures);
        if (t == null) return null;
        tl.terms.add(t);
        // TODO: better parsing of lists
        pr.consume(",");
      }
      pr.read();
      return tl;
    }
    return null;
  }

  @Override
  public String toString() {
    return terms.toString();
  }

  public static void main(String[] args) {
    Term tl = read(new ParserReader("[a,b]"), new HashMap<>(), Parseable.ALL_FEATURES);
    System.out.println(tl);
  }

  @Override
  public Stream<Variable> getVariables() {
    return terms.stream().flatMap(Term::getVariables);
  }

  @Override
  public int compareTo(Object o) {
    return toString().compareTo(o.toString()); //TODO: inefficient
  }

  @Override
  public int hashCode() {
    return terms.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof TermList && Objects.equals(terms, ((TermList) obj).terms);
  }

  public List<Term> terms() {
    return terms;
  }
}
