package common.plan;

import org.junit.Assert;
import org.junit.Test;

public class PlanNodeTest {

  @Test
  public void testSimplify() {
    TableNode foo = new TableNode("foo", 2);
    TableNode bar = new TableNode("bar", 2);
    Assert.assertEquals(
            new UnionNode(foo, bar),
            new UnionNode(foo, new UnionNode(bar, new UnionNode())).simplify()
    );
    Assert.assertEquals(
            foo,
            new UnionNode(foo, new UnionNode(foo, new UnionNode())).simplify()
    );
    Assert.assertEquals(
            foo,
            new ProjectNode(foo, new int[]{0, 1}).simplify()
    );
  }
}
