package common.parser;

import java.util.*;
import java.util.stream.Collectors;

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

  public static Rule read(ParserReader pr) {
    Map<String, Variable> variables = new HashMap<>();

    pr.skipComments();
    if (pr.peek() == null) return null;
    // parse head
    CompoundTerm head = CompoundTerm.read(pr, variables);
    if (head == null) return null;
    // parse body
    List<CompoundTerm> body = new ArrayList<>();

    if (pr.consume(".") != null) {
      return new Rule(head);
    }

    String found = pr.expect(":-", ":~", "<-", "<~");
    switch (found) {
      case ":-":
      case "<-":
        do {
          CompoundTerm b = CompoundTerm.read(pr, variables);
          if (b == null) return null;
          body.add(b);
          if (pr.consume(".") != null) {
            break;
          }
        } while (pr.expect(",") != null);
        break;
      case ":~":
      case "<~":
        CompoundTerm ct = new CompoundTerm("bash_command");
        pr.skipComments();
        Constant<String> c = new Constant<>(pr.readLine());
        TermList tl = new TermList(Arrays.stream(head.args).filter(t -> t instanceof Variable).toArray(Term[]::new));
        ct.args = new Term[] { c, tl };
        body.add(ct);
        break;
    }
    return new Rule(head, body);
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(head).append(" :- ");
    for (CompoundTerm a : body) {
      b.append(a).append(", ");
    }
    if (!body.isEmpty()) b.setLength(b.length() - 2);
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
    r = read(pr = new ParserReader("pred(A) :- pred2(a,b)."));
    System.out.println(r);
    System.out.println("remaining: " + pr.peekLine());
    r = read(pr = new ParserReader("pred(A) :- pred2([A,B])."));
    System.out.println(r);
    System.out.println("remaining: " + pr.peekLine());
    r = read(pr = new ParserReader("pred(A) :~ cat a.txt"));
    System.out.println(r);
  }

}
