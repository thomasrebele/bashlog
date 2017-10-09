package common.plan;

import common.parser.CompoundTerm;
import common.parser.TermList;
import common.parser.Variable;
import org.junit.Assert;
import org.junit.Test;

public class PlanNodeTest {

  @Test
  public void testPlanNode() {
    TermList args2 = new TermList(new Variable("X"), new Variable("Y"));
    PlanNode foo = new BuiltinNode(new CompoundTerm("foo", args2));
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args2));
    Assert.assertTrue((new VariableEqualityFilterNode(foo, 1, 1)).contains(foo));
    Assert.assertFalse((new VariableEqualityFilterNode(foo, 1, 1)).contains(bar));
    Assert.assertEquals(
            new VariableEqualityFilterNode(foo, 1, 1),
            (new VariableEqualityFilterNode(bar, 1, 1)).replace(bar, foo)
    );
  }
  @Test
  public void testSimplifier() {
    PlanSimplifier simplifier = new PlanSimplifier();
    TermList args2 = new TermList(new Variable("X"), new Variable("Y"));
    TermList args4 = new TermList(new Variable("X"), new Variable("Y"), new Variable("Z"), new Variable("W"));
    PlanNode foo = new BuiltinNode(new CompoundTerm("foo", args2));
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args2));
    PlanNode baz = new BuiltinNode(new CompoundTerm("bar", args4));
    Assert.assertEquals(
            new UnionNode(foo, bar),
            simplifier.apply(new UnionNode(foo, new UnionNode(bar, PlanNode.empty(2))))
    );
    Assert.assertEquals(
            foo,
            simplifier.apply(new UnionNode(foo, new UnionNode(foo, PlanNode.empty(2))))
    );
    Assert.assertEquals(
            foo,
            simplifier.apply(new ProjectNode(foo, new int[]{0, 1}))
    );
    Assert.assertEquals(
            PlanNode.empty(2),
            simplifier.apply(new ProjectNode(PlanNode.empty(2), new int[]{0, 0}))
    );
    Assert.assertEquals(
            new ProjectNode(baz, new int[]{0, 1, 3}),
            simplifier.apply(new ProjectNode(
                    new ProjectNode(baz, new int[]{3, 0, 1}),
                    new int[]{1, 2, 0}))
    );

    RecursionNode recursionNode = new RecursionNode(PlanNode.empty(2));
    recursionNode.addRecursivePlan(new UnionNode(recursionNode.getDelta(), foo));
    Assert.assertTrue(simplifier.apply(recursionNode) instanceof RecursionNode);

    RecursionNode recursionNode2 = new RecursionNode(PlanNode.empty(2));
    recursionNode2.addRecursivePlan(recursionNode2.getDelta());
    Assert.assertEquals(PlanNode.empty(2), simplifier.apply(recursionNode2));
  }

  @Test
  public void testPushDownFilterOptimizer() {
    Optimizer optimizer = new PushDownFilterOptimizer();
    TermList args = new TermList(new Variable("X"), new Variable("Y"));
    PlanNode foo = new BuiltinNode(new CompoundTerm("foo", args));
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args));
    PlanNode fooFilter = new ConstantEqualityFilterNode(foo, 0, "foo");
    PlanNode barFilter = new ConstantEqualityFilterNode(bar, 0, "foo");

    Assert.assertEquals(
            new UnionNode(fooFilter, barFilter),
            optimizer.apply(new ConstantEqualityFilterNode(new UnionNode(foo, bar), 0, "foo"))
    );

    Assert.assertEquals(
            new ProjectNode(fooFilter, new int[]{1, 0}),
            optimizer.apply(new ConstantEqualityFilterNode(new ProjectNode(foo, new int[]{1, 0}), 1, "foo"))
    );
    Assert.assertEquals(
            new ProjectNode(foo, new int[]{0, -1}, new Comparable[]{null, "foo"}),
            optimizer.apply(new ConstantEqualityFilterNode(new ProjectNode(foo, new int[]{0, -1}, new Comparable[]{null, "foo"}), 1, "foo"))
    );
    Assert.assertEquals(
            PlanNode.empty(2),
            optimizer.apply(new ConstantEqualityFilterNode(new ProjectNode(foo, new int[]{0, -1}, new Comparable[]{null, "bar"}), 1, "foo"))
    );

    Assert.assertEquals(
            new JoinNode(fooFilter, bar, new int[]{1}, new int[]{1}),
            optimizer.apply(new ConstantEqualityFilterNode(new JoinNode(foo, bar, new int[]{1}, new int[]{1}), 0, "foo"))
    );
    Assert.assertEquals(
            new JoinNode(foo, barFilter, new int[]{1}, new int[]{1}),
            optimizer.apply(new ConstantEqualityFilterNode(new JoinNode(foo, bar, new int[]{1}, new int[]{1}), 2, "foo"))
    );
    Assert.assertEquals(
            new JoinNode(fooFilter, barFilter, new int[]{0}, new int[]{0}),
            optimizer.apply(new ConstantEqualityFilterNode(new JoinNode(foo, bar, new int[]{0}, new int[]{0}), 0, "foo"))
    );
    Assert.assertEquals(
            new JoinNode(fooFilter, barFilter, new int[]{0}, new int[]{0}),
            optimizer.apply(new ConstantEqualityFilterNode(new JoinNode(foo, bar, new int[]{0}, new int[]{0}), 2, "foo"))
    );
  }
}
