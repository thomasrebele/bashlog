package common.plan;

import org.junit.Assert;
import org.junit.Test;

public class PlanNodeTest {

  @Test
  public void testSimplify() {
    Assert.assertEquals(
        new UnionNode(new TableNode("foo", 2), new TableNode("bar", 2)),
        new UnionNode(new TableNode("foo", 2), new UnionNode(new TableNode("bar", 2), new UnionNode())).simplify()
    );
    Assert.assertEquals(
        new TableNode("foo", 2), new UnionNode(new TableNode("foo", 2), new UnionNode(new TableNode("foo", 2), new UnionNode())).simplify()
    );
  }
}
