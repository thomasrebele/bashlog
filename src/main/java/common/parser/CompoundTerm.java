package common.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompoundTerm implements Parseable {

  public final String name;

  public Term[] args;

  @Override
  public String toString() {
    return (name + Arrays.toString(args).replace('[', '(').replace(']', ')'));
  }

  public CompoundTerm(String name) {
    this.name = name;
  }

  public static CompoundTerm read(ParserReader pr, Map<String, Variable> varMap) {
    pr.debug();
    pr.skipComments();
    String name = pr.readName();
    if (name == null) return null;
    CompoundTerm a = new CompoundTerm(name);
    pr.debug();
    Term v = null;
    pr.skipComments();
    List<Term> args = new ArrayList<>();
    if (pr.expect("(") != null) {
      while ((v = Term.read(pr, varMap)) != null) {
        args.add(v);
        pr.skipComments();
        if (pr.peek() == ')' || pr.expect(",") == null) {
          break;
        }
      }
      pr.expect(")");
    } else {
      return null;
    }
    a.args = args.toArray(new Term[] {});
    return a;
  }

  public String signature() {
    return name + "/" + args.length;
  }

  public static void main(String[] args) {
    ParserReader pr;
    CompoundTerm a = CompoundTerm.read(pr = new ParserReader("abc(\"a\")"), new HashMap<>());
    System.out.println(a);
    System.out.println("remaining: " + pr.peekLine());
    a = CompoundTerm.read(pr = new ParserReader("abc(a)"), new HashMap<>());
    System.out.println(a);
    System.out.println("remaining: " + pr.peekLine());
    a = CompoundTerm.read(pr = new ParserReader("abc(A)"), new HashMap<>());
    System.out.println(a);
    System.out.println("remaining: " + pr.peekLine());
    //System.out.println(YCompoundTerm.read("abc(\"a\", b)", new int[] { 0 }, new HashMap<>(), null));
  }

}