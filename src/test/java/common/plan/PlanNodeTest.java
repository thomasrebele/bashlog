package common.plan;

import common.parser.CompoundTerm;
import common.parser.TermList;
import common.parser.Variable;
import org.junit.Assert;
import org.junit.Test;

public class PlanNodeTest {

  @Test
  public void testSimplify() {
    TermList args = new TermList(new Variable("X"), new Variable("Y"));
    PlanNode foo = new BuiltinNode(new CompoundTerm("foo", args));
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args));
    Assert.assertEquals(
            new UnionNode(foo, bar),
            new UnionNode(foo, new UnionNode(bar, new UnionNode(2))).simplify()
    );
    Assert.assertEquals(
            foo,
            new UnionNode(foo, new UnionNode(foo, new UnionNode(2))).simplify()
    );
    Assert.assertEquals(
            foo,
            new ProjectNode(foo, new int[]{0, 1}).simplify()
    );
    Assert.assertEquals(
            new UnionNode(2),
            new ProjectNode(new UnionNode(2), new int[]{0, 0}).simplify()
    );
  }
}
