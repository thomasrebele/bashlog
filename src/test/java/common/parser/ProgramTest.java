package common.parser;

import org.junit.Assert;
import org.junit.Test;

public class ProgramTest {

  @Test
  public void testHasAncestor() {
    Program program = Program.read(new ParserReader(
            "a(X,Z) :- b(X,Y), in(Y,Z). b(X,Z) :- b(X,Y), in(Y,Z). b(X,Z) :- a(X,Y), in(Y,Z)."
    ));
    Assert.assertFalse(program.hasAncestor("in/2", "a/2"));
    Assert.assertTrue(program.hasAncestor("a/2", "b/2"));
    Assert.assertTrue(program.hasAncestor("b/2", "b/2"));
    Assert.assertTrue(program.hasAncestor("b/2", "a/2"));
  }
}
