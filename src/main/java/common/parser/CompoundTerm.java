package common.parser;

import java.util.*;
import java.util.stream.Stream;

public class CompoundTerm extends Term implements Parseable {

  public final String name;

  public Term[] args;

  public final boolean negated;

  public CompoundTerm(String name) {
    this(name, false, new Term[]{});
  }

  public CompoundTerm(String name, boolean negated) {
    this(name, negated, new Term[]{});
  }

  public CompoundTerm(String name, Term... args) {
    this(name, false, args);
  }

  public CompoundTerm(String name, boolean negated, Term... args) {
    this.name = name;
    this.negated = negated;
    this.args = args;
  }


  public static CompoundTerm read(ParserReader pr, Map<String, Variable> varMap, Set<String> supportedFeatures) {
    pr.debug();
    pr.skipComments();
    boolean negated = (pr.consume("not") != null);
    pr.skipComments();
    String name = pr.readName();
    if (name == null) return null;
    return read(name, negated, pr, varMap, supportedFeatures);
  }

  public static CompoundTerm read(String name, boolean negated, ParserReader pr, Map<String, Variable> varMap, Set<String> supportedFeatures) {
    CompoundTerm a = new CompoundTerm(name, negated);
    pr.debug();
    Term v;
    pr.skipComments();
    List<Term> args = new ArrayList<>();
    if (pr.expect("(") != null) {
      while ((v = Term.read(pr, varMap, supportedFeatures)) != null) {
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
  public String toString() {
    return ( negated ? "not " : "") + (name + Arrays.toString(args).replace('[', '(').replace(']', ')'));
  }

  @Override
  public int hashCode() {
    return name.hashCode() ^ Arrays.hashCode(args);
  }

  public String getRelation() {
    return name + "/" + args.length;
  }

  @Override
  public Stream<Variable> getVariables() {
    return Arrays.stream(args).flatMap(Term::getVariables);
  }

  public static void main(String[] args) {
    ParserReader pr;
    CompoundTerm a = CompoundTerm.read(pr = new ParserReader("abc(\"a\")"), new HashMap<>(), Parseable.ALL_FEATURES);
    System.out.println(a);
    System.out.println("remaining: " + pr.peekLine());
    a = CompoundTerm.read(pr = new ParserReader("abc(a)"), new HashMap<>(), Parseable.ALL_FEATURES);
    System.out.println(a);
    System.out.println("remaining: " + pr.peekLine());
    a = CompoundTerm.read(pr = new ParserReader("abc(A)"), new HashMap<>(), Parseable.ALL_FEATURES);
    System.out.println(a);
    System.out.println("remaining: " + pr.peekLine());
    //System.out.println(YCompoundTerm.read("abc(\"a\", b)", new int[] { 0 }, new HashMap<>(), null));
  }

  @Override
  public int compareTo(Object o) {
    throw new UnsupportedOperationException();
  }
}
