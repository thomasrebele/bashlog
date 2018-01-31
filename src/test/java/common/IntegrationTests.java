package common;

import common.parser.ParserReader;
import common.parser.Program;
import org.junit.Assert;
import org.junit.Test;

public class IntegrationTests {

  protected Evaluator eval;

  public IntegrationTests(Evaluator eval) {
    this.eval = eval;
  }

  @Test
  public void testSuperSimple() throws Exception {
    Program program = Program.read(new ParserReader("out(X,Y) :- in(X,Y)."));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("in/2", "A", "B");

    FactsSet result = eval.evaluate(program, facts, Tools.set("out/2"));
    Assert.assertEquals(1, result.getByRelation("out/2").count());
    Assert.assertEquals(1, result.getRelations().size());
  }

  @Test
  public void testSameArgument() throws Exception {
    Program program = Program.read(new ParserReader("bad() :- parent(X,X)."));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("parent/2", "bob", "alice");
    facts.add("parent/2", "charly", "alice");

    FactsSet result = eval.evaluate(program, facts, Tools.set("bad/0"));

    Assert.assertEquals(0, result.getByRelation("bad/0").count());
  }

  @Test
  public void testSimple() throws Exception {
    Program program = Program.read(new ParserReader(
            "sibling(X,Y) :- parent(X,Z), parent(Y,Z). bad(X,X) :- parent(X,X). bobParent(X) :- parent(\"bob\", X)."
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("parent/2", "bob", "alice");
    facts.add("parent/2", "charly", "alice");

    FactsSet result = eval.evaluate(program, facts, Tools.set("bad/2", "sibling/2", "bobParent/1"));

    Assert.assertEquals(0, result.getByRelation("bad/2").count());
    Assert.assertEquals(4, result.getByRelation("sibling/2").count());
    Assert.assertEquals(1, result.getByRelation("bobParent/1").count());
  }

  @Test
  public void testLinearClosure() throws Exception {
    Program program = Program.read(new ParserReader(
            "ancestor(X,Y) :- parent(X,Y). ancestor(X,Z) :- ancestor(X,Y), parent(Y,Z)."
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("parent/2", "bob", "alice");
    facts.add("parent/2", "alice", "charly");
    facts.add("parent/2", "charly", "david");

    FactsSet result = eval.evaluate(program, facts, Tools.set("ancestor/2"));

    Assert.assertEquals(6, result.getByRelation("ancestor/2").count());
  }

  @Test
  public void testSymmetricClosure() throws Exception {
    Program program = Program.read(new ParserReader(
            "ancestor(X,Y) :- parent(X,Y). ancestor(X,Z) :- ancestor(X,Y), ancestor(Y,Z)."
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("parent/2", "bob", "alice");
    facts.add("parent/2", "alice", "charly");
    facts.add("parent/2", "charly", "david");

    FactsSet result = eval.evaluate(program, facts, Tools.set("ancestor/2"));

    Assert.assertEquals(6, result.getByRelation("ancestor/2").count());
  }

  @Test
  public void testSize2Loop() throws Exception {
    Program program = Program.read(new ParserReader(
            "bar(X,Y) :- foo(X,Y). baz(X,Z) :- bar(X,Y), foo(Y,Z). bar(X,Z) :- baz(X,Y), foo(Y,Z)."
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("foo/2", "alice", "bob");
    facts.add("foo/2", "bob", "charly");
    facts.add("foo/2", "charly", "dave");
    facts.add("foo/2", "dave", "eve");

    FactsSet result = eval.evaluate(program, facts, Tools.set("baz/2"));

    Assert.assertEquals(4, result.getByRelation("baz/2").count());
  }

  @Test
  public void testInnerLoop() throws Exception {
    Program program = Program.read(new ParserReader(
            "a(X,Y) :- in(X,Y). a(X,Z) :- b(X,Y), in(Y,Z). b(X,Z) :- in(X,Y), b(Y,Z). b(X,Z) :- in(X,Y), c(Y,Z). c(X,Z) :- a(X,Y), in(Y,Z)."
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("in/2", "bob", "alice");
    facts.add("in/2", "alice", "charly");
    facts.add("in/2", "charly", "david");

    FactsSet result = eval.evaluate(program, facts, Tools.set("a/2"));

    Assert.assertEquals(3, result.getByRelation("a/2").count());
  }

  @Test
  public void testReadFile() throws Exception {
    Program program = Program.loadFile("data/bashlog/recursion/datalog.txt");
    FactsSet result = eval.evaluate(program, new SimpleFactsSet(), Tools.set("tc/2"));
    Assert.assertEquals(28, result.getByRelation("tc/2").count());
  }

  @Test
  public void endless() throws Exception {
    Program program = Program.read(new ParserReader(//
        "a(X,Y) :- b(X,Y)." + //
            "b(X,Y) :- a(X,Y)." //
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("a/2", "1", "2");

    FactsSet result = eval.evaluate(program, facts, Tools.set("a/2"));
    Assert.assertEquals(1, result.getByRelation("a/2").count());
  }

  @Test
  public void endless2() throws Exception {
    Program program = Program.read(new ParserReader(//
        "a(X,Y) :- b(X,Y)." + //
            "b(X,Y) :- c(X,Y)." + //
            "c(X,Y) :- a(X,Y)." //
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("a/2", "1", "2");

    FactsSet result = eval.evaluate(program, facts, Tools.set("a/2"));
    Assert.assertEquals(1, result.getByRelation("a/2").count());
  }

  @Test
  public void negation() throws Exception {
    Program program = Program.read(new ParserReader("a(X,Y) :- not c(X), b(X, Y)."));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("b/2", "1", "2");
    facts.add("b/2", "2", "3");
    facts.add("b/2", "2", "4");
    facts.add("c/1", "2");

    FactsSet result = eval.evaluate(program, facts, Tools.set("a/2"));
    Assert.assertEquals(1, result.getByRelation("a/2").count());
  }

  @Test
  public void negationMultipleVars() throws Exception {
    Program program = Program.read(new ParserReader("a(X,Y) :- not c(X,Y), b(X, Y)."));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("b/2", "1", "2");
    facts.add("b/2", "2", "3");
    facts.add("c/2", "2", "3");

    FactsSet result = eval.evaluate(program, facts, Tools.set("a/2"));
    Assert.assertEquals(1, result.getByRelation("a/2").count());
  }

  @Test
  public void multipleNegations() throws Exception {
    Program program = Program.read(new ParserReader("a(X,Y) :- b(X, Y), not c(X), not d(Y)."));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("b/2", "1", "2");
    facts.add("b/2", "2", "3");
    facts.add("b/2", "2", "4");
    facts.add("c/1", "1");
    facts.add("d/1", "3");

    FactsSet result = eval.evaluate(program, facts, Tools.set("a/2"));
    Assert.assertEquals(1, result.getByRelation("a/2").count());
  }

  @Test
  public void negationBounded() throws Exception {
    Program program = Program.read(new ParserReader("a(X,Y) :- b(X, Y), not c(X, Z), d(Z)."));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("b/2", "1", "2");
    facts.add("b/2", "2", "3");
    facts.add("c/2", "1", "3");
    facts.add("d/1", "3");

    FactsSet result = eval.evaluate(program, facts, Tools.set("a/2"));
    Assert.assertEquals(1, result.getByRelation("a/2").count());
  }
}
