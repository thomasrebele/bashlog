package common;

import common.parser.ParserReader;
import common.parser.Program;
import flinklog.FactsSet;
import flinklog.SimpleFactsSet;
import org.apache.commons.compress.utils.Sets;
import org.junit.Assert;

public class IntegrationTests {

  public static void testSimple(Evaluator eval) throws Exception {
    Program program = Program.read(new ParserReader(
            "sibling(X,Y) :- parent(X,Z), parent(Y,Z). bad(X,X) :- parent(X,X). bobParent(X) :- parent(\"bob\", X)."
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("parent/2", "bob", "alice");
    facts.add("parent/2", "charly", "alice");

    FactsSet result = eval.evaluate(program, facts, Sets.newHashSet("bad/2", "sibling/2", "bobParent/1"));

    Assert.assertEquals(0, result.getByRelation("bad/2").count());
    Assert.assertEquals(4, result.getByRelation("sibling/2").count());
    Assert.assertEquals(1, result.getByRelation("bobParent/1").count());
  }

  public static void testLinearClosure(Evaluator eval) throws Exception {
    Program program = Program.read(new ParserReader(
            "ancestor(X,Y) :- parent(X,Y). ancestor(X,Z) :- ancestor(X,Y), parent(Y,Z)."
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("parent/2", "bob", "alice");
    facts.add("parent/2", "alice", "charly");
    facts.add("parent/2", "charly", "david");

    FactsSet result = eval.evaluate(program, facts, Sets.newHashSet("ancestor/2"));

    Assert.assertEquals(6, result.getByRelation("ancestor/2").count());
  }

  public static void testSymmetricClosure(Evaluator eval) throws Exception {
    Program program = Program.read(new ParserReader(
            "ancestor(X,Y) :- parent(X,Y). ancestor(X,Z) :- ancestor(X,Y), ancestor(Y,Z)."
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("parent/2", "bob", "alice");
    facts.add("parent/2", "alice", "charly");
    facts.add("parent/2", "charly", "david");

    FactsSet result = eval.evaluate(program, facts, Sets.newHashSet("ancestor/2"));

    Assert.assertEquals(6, result.getByRelation("ancestor/2").count());
  }

  public static void testSize2Loop(Evaluator eval) throws Exception {
    Program program = Program.read(new ParserReader(
            "bar(X,Y) :- foo(X,Y). baz(X,Z) :- bar(X,Y), foo(Y,Z). bar(X,Z) :- baz(X,Y), foo(Y,Z)."
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("foo/2", "bob", "alice");
    facts.add("foo/2", "alice", "charly");
    facts.add("foo/2", "charly", "david");

    FactsSet result = eval.evaluate(program, facts, Sets.newHashSet("baz/2"));

    Assert.assertEquals(4, result.getByRelation("baz/2").count());
  }

  public static void testInnerLoop(Evaluator eval) throws Exception {
    Program program = Program.read(new ParserReader(
            "a(X,Y) :- in(X,Y). a(X,Z) :- b(X,Y), in(Y,Z). b(X,Z) :- b(X,Y), in(Y,Z). b(X,Z) :- c(X,Y), in(Y,Z). c(X,Z) :- a(X,Y), in(Y,Z)."
    ));
    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("in/2", "bob", "alice");
    facts.add("in/2", "alice", "charly");
    facts.add("in/2", "charly", "david");

    FactsSet result = eval.evaluate(program, facts, Sets.newHashSet("a/2"));

    Assert.assertEquals(-1, result.getByRelation("a/2").count()); //TODO: compter le résultat attendu
  }

  public static void testReadFile(Evaluator eval) throws Exception {
    Program program = Program.loadFile("data/bashlog/recursion/datalog.txt");
    FactsSet result = eval.evaluate(program, new SimpleFactsSet(), Sets.newHashSet("tc/2"));
    Assert.assertEquals(28, result.getByRelation("tc/2").count());
  }
}
