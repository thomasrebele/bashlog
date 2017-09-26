package common.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TermList extends Term {

  List<Term> terms = new ArrayList<>();

  public static TermList read(ParserReader pr, Map<String, Variable> varMap) {
    if (pr.consume("[") != null) {
      TermList tl = new TermList();
      while (pr.peek() != ']') {
        Term t = Term.read(pr, varMap);
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
    Term tl = read(new ParserReader("[a,b]"), new HashMap<>());
    System.out.println(tl);
  }

}
