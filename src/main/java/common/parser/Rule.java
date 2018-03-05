package common.parser;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Rule implements Parseable {

  public CompoundTerm head;

  public List<CompoundTerm> body;

  public Rule(CompoundTerm head, List<CompoundTerm> body) {
    this.head = head;
    this.body = body;
  }

  public Rule(CompoundTerm head, CompoundTerm... body) {
    this(head, Arrays.asList(body));
  }

  public static Rule read(ParserReader pr, Set<String> supportedFeatures) {
    Map<String, Variable> variables = new HashMap<>();

    pr.skipComments();
    if (pr.peek() == null) return null;
    // parse head
    CompoundTerm head = CompoundTerm.read(pr, variables, supportedFeatures);
    if (head == null) return null;
    // parse body
    List<CompoundTerm> body = new ArrayList<>();

    if (pr.consume(".") != null) {
      return new Rule(head);
    }

    String found = pr.expect(":-", ":~", "<-", "<~", ".");
    switch (found) {
      case ":-":
      case "<-":
        loop: do {
          CompoundTerm b = CompoundTerm.read(pr, variables, supportedFeatures);
          if (b == null) {
            pr.error(new String[] { "expected: term(...)" }, null);
          }
          body.add(b);
          switch (pr.expect(",", ".")) {
            case ",":
              break;
            case ".":
              break loop;
          }
        } while (true);
        break;
      case ":~":
      case "<~":
        return BashRule.read(pr, supportedFeatures, head);
      case ".":
        break;
    }
    Set<Variable> headVars = head.getVariables().collect(Collectors.toSet());
    headVars.removeAll(body.stream().flatMap(ct -> ct.getVariables()).collect(Collectors.toList()));
    if (headVars.size() > 0) {
      pr.error("head variable(s) not used in body: " + headVars.stream().map(v -> v.name).collect(Collectors.joining(", ")), null);
    }

    return new Rule(head, body);
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(head);
    if (body.size() > 0) {
      b.append(" :- ");
      for (CompoundTerm a : body) {
        b.append(a).append(", ");
      }
    }
    if (!body.isEmpty()) b.setLength(b.length() - 2);
    b.append(".");
    return (b.toString());
  }

  public Set<String> getDependencies() {
    // TODO: nested terms
    return body.stream().map(CompoundTerm::getRelation).collect(Collectors.toSet());
  }

  public static void main(String[] args) {
    Rule r;
    /*r = read(new ParserReader("pred(a) :- trans(a,b), trans(b,_)."), null);
    System.out.println(r);*/
    ParserReader pr;
    r = read(pr = new ParserReader("pred(A) :- pred2(a,b)."), Parseable.ALL_FEATURES);
    System.out.println(r);
    System.out.println("remaining: " + pr.peekLine());
    r = read(pr = new ParserReader("pred(A) :- pred2([A,B])."), Parseable.ALL_FEATURES);
    System.out.println(r);
    System.out.println("remaining: " + pr.peekLine());
    r = read(pr = new ParserReader("pred(A) :~ cat a.txt"), Parseable.ALL_FEATURES);
    System.out.println(r);
  }

  public static Rule bashRule(String relation, String cmd) {
    String[] rel = relation.split("/");
    int arity = Integer.parseInt(rel[1]);
    Term[] args = IntStream.range(0, arity).mapToObj(tmpI -> new Variable("tmp_" + tmpI)).toArray(Term[]::new);
    return BashRule.read(new ParserReader(cmd), Parseable.ALL_FEATURES, new CompoundTerm(rel[0], args));

    /*CompoundTerm ct = new CompoundTerm("bash_command");
    Constant<String> c = new Constant<>(cmd);
    ct.args = new Term[] { c, new TermList(args) };
    Rule r = new Rule(new CompoundTerm(rel[0], args), ct);
    return r;*/
  }

}
