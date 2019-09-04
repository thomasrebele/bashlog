package sqllog;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import common.parser.ParserReader;
import common.parser.Program;

public class IntegrationTests {

  @Test
  public void testSimple() {
    Program program = Program.read(new ParserReader(
            "sibling(X,Y) :- parent(X,Z), parent(Y,Z). bad(X,X) :- parent(X,X). bobParent(X) :- parent(\"bob\", X)."
    ));
    
    Assert.assertEquals(
            "SELECT T1.C0, T2.C0 FROM parent AS T1, parent AS T2 WHERE T1.C1 = T2.C1",
            (new SqllogCompiler()).compile(program, Collections.singleton("parent/2"), "sibling/2")
    );
    Assert.assertEquals(
            "SELECT T1.C1 FROM parent AS T1 WHERE T1.C0 = 'bob'",
            (new SqllogCompiler()).compile(program, Collections.singleton("parent/2"), "bobParent/1")
    );
  }

  @Test
  public void testLinearClosure() {
    Program program = Program.read(new ParserReader(
            "ancestor(X,Y) :- parent(X,Y). ancestor(X,Z) :- ancestor(X,Y), parent(Y,Z)."
    ));

    Assert.assertEquals(
            "WITH RECURSIVE T1(C0, C1) AS ((SELECT T2.C0, T2.C1 FROM parent AS T2) UNION ALL (SELECT T3.C0, T4.C1 FROM T1 AS T3, parent AS T4 WHERE T3.C1 = T4.C0)) SELECT T5.C0, T5.C1 FROM T1 AS T5",
            (new SqllogCompiler()).compile(program, Collections.singleton("parent/2"), "ancestor/2")
    );
  }
}
