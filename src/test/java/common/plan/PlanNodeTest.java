package common.plan;

import org.junit.Assert;
import org.junit.Test;

public class PlanNodeTest {

  @Test
  public void testSimplify() {
    Assert.assertEquals(
            new UnionNode(new TableNode("foo"), new TableNode("bar")),
            new UnionNode(new TableNode("foo"), new UnionNode(new TableNode("bar"), new UnionNode())).simplify()
    );
    Assert.assertEquals(
            new TableNode("foo"),
            new UnionNode(new TableNode("foo"), new UnionNode(new TableNode("foo"), new UnionNode())).simplify()
    );
  }
}
