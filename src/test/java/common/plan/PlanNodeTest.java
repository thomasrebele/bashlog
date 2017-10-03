package common.plan;

import common.parser.CompoundTerm;
import common.parser.TermList;
import common.parser.Variable;
import org.junit.Assert;
import org.junit.Test;

public class PlanNodeTest {

  @Test
  public void testSimplifier() {
    PlanSimplifier simplifier = new PlanSimplifier();
    TermList args = new TermList(new Variable("X"), new Variable("Y"));
    PlanNode foo = new BuiltinNode(new CompoundTerm("foo", args));
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args));
    Assert.assertEquals(
            new UnionNode(foo, bar),
            simplifier.simplify(new UnionNode(foo, new UnionNode(bar, new UnionNode(2))))
    );
    Assert.assertEquals(
            foo,
            simplifier.simplify(new UnionNode(foo, new UnionNode(foo, new UnionNode(2))))
    );
    Assert.assertEquals(
            foo,
            simplifier.simplify(new ProjectNode(foo, new int[]{0, 1}))
    );
    Assert.assertEquals(
            new UnionNode(2),
            simplifier.simplify(new ProjectNode(new UnionNode(2), new int[]{0, 0}))
    );
  }
}
