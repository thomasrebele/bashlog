package common.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Rule implements Parseable {

  public CompoundTerm head;

  public List<CompoundTerm> body;

  protected Map<String, Variable> variables = new HashMap<>();

  protected static Rule read(ParserReader pr) {
    Rule r = new Rule();
    pr.skipComments();
    if (pr.peek() == null) return null;
    // parse head
    r.head = CompoundTerm.read(pr, r.variables);
    if (r.head == null) return null;
    // parse body
    r.body = new ArrayList<>();

    if (pr.consume(".") != null) {
      return r;
    }

    String found = pr.expect(":-", ":~");
    switch (found) {
      case ":-":
        do {
          CompoundTerm b = CompoundTerm.read(pr, r.variables);
          if (b == null) return null;
          r.body.add(b);
          if (pr.consume(".") != null) {
            break;
          }
        } while (pr.expect(",") != null);
        break;
      case ":~":
        CompoundTerm ct = new CompoundTerm("bash_command");
        Constant c = new Constant();
        pr.skipComments();
        c.value = pr.readLine();
        List<Term> args = new ArrayList<>();
        args.add(c);
        for (Term arg : r.head.args) {
          if (arg instanceof Variable) {
            args.add(arg);
          }
        }
        ct.args = args.toArray(ct.args);
        r.body.add(ct);
        break;
    }
    return r;
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

  public int variableCount() {
    return variables.size();
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
