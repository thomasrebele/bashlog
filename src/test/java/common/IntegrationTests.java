package common;

import org.junit.Assert;
import org.junit.Test;

import common.parser.ParserReader;
import common.parser.Program;
import flinklog.FactsSet;
import flinklog.SimpleFactsSet;

public class IntegrationTests {

  protected Evaluator eval;

  public IntegrationTests(Evaluator eval) {
    this.eval = eval;
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
    facts.add("foo/2", "bob", "alice");
    facts.add("foo/2", "alice", "charly");
    facts.add("foo/2", "charly", "david");

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
}
