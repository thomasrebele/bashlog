package common.parser;

import java.util.*;
import java.util.stream.Stream;

public class CompoundTerm extends Term implements Parseable {

  public final String name;

  public Term[] args;

  @Override
  public String toString() {
    return (name + Arrays.toString(args).replace('[', '(').replace(']', ')'));
  }

  public CompoundTerm(String name) {
    this.name = name;
  }

  public CompoundTerm(String name, Term... args) {
    this.name = name;
    this.args = args;
  }

  public static CompoundTerm read(ParserReader pr, Map<String, Variable> varMap) {
    pr.debug();
    pr.skipComments();
    String name = pr.readName();
    if (name == null) return null;
    CompoundTerm a = new CompoundTerm(name);
    pr.debug();
    Term v;
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

  @Override
  public boolean equals(Object obj) {
    return obj instanceof CompoundTerm && name.equals(((CompoundTerm) obj).name) && Arrays.equals(args, ((CompoundTerm) obj).args);
  }

  @Override
  public int hashCode() {
    return name.hashCode() ^ Arrays.hashCode(args);
  }

  public String signature() {
    return name + "/" + args.length;
  }

  @Override
  public Stream<Variable> getVariables() {
    return Arrays.stream(args).flatMap(Term::getVariables);
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

  @Override
  public int compareTo(Object o) {
    throw new UnsupportedOperationException();
  }

}