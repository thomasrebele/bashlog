package common.plan;

import common.parser.CompoundTerm;
import common.parser.TermList;
import common.parser.Variable;
import common.plan.node.*;
import common.plan.optimizer.*;

import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;

public class PlanNodeTest {

  TermList args2 = new TermList(new Variable("X"), new Variable("Y"));

  TermList args3 = new TermList(new Variable("X"), new Variable("Y"), new Variable("Z"));

  TermList args4 = new TermList(new Variable("X"), new Variable("Y"), new Variable("Z"), new Variable("W"));

  TermList args5 = new TermList(new Variable("X"), new Variable("Y"), new Variable("Z"), new Variable("W"), new Variable("V"));

  @Test
  public void testPlanNode() {
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
    SimplifyPlan simplifier = new SimplifyPlan();
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

    Assert.assertEquals(
            PlanNode.empty(4),
            simplifier.apply(new JoinNode(foo, PlanNode.empty(2), new int[]{0}, new int[]{0}))
    );
    Assert.assertEquals(
            simplifier.apply(new JoinNode(foo, bar, new int[]{0}, new int[]{0})),
            simplifier.apply(new JoinNode(foo, bar, new int[]{0}, new int[]{0}))
    );

    RecursionNode recursionNode = new RecursionNode(foo);
    recursionNode.addRecursivePlan(recursionNode.getDelta().join(bar, new int[]{0}, new int[]{0}).project(new int[]{0, 1}));
    Assert.assertTrue(simplifier.apply(recursionNode) instanceof RecursionNode);

    RecursionNode recursionNode2 = new RecursionNode(PlanNode.empty(2));
    recursionNode2.addRecursivePlan(recursionNode2.getDelta());
    Assert.assertEquals(PlanNode.empty(2), simplifier.apply(recursionNode2));

    RecursionNode recursionNode3 = new RecursionNode(foo);
    recursionNode3.addRecursivePlan(bar);
    Assert.assertEquals(foo.union(bar), simplifier.apply(recursionNode3));

    RecursionNode recursionNode4 = new RecursionNode(PlanNode.empty(2));
    recursionNode4.addRecursivePlan(foo.union(recursionNode4.getDelta()));
    Assert.assertEquals(foo, simplifier.apply(recursionNode4));
  }

  @Test
  public void testPushDownFilterOptimizer() {
    Optimizer optimizer = new PushDownFilter();
    PlanNode foo = new BuiltinNode(new CompoundTerm("foo", args2));
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args2));
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

  @Test
  public void testPushDownFilterOptimizer2() {
    Optimizer optimizer = new PushDownFilter();

    PlanNode baz = new BuiltinNode(new CompoundTerm("baz", args3));
    PlanNode bazFilter1 = new ConstantEqualityFilterNode(baz, 1, "foo1");
    PlanNode bazFilter2 = new ConstantEqualityFilterNode(baz, 1, "foo2");
    Assert.assertEquals(new UnionNode(//
        new ProjectNode(bazFilter1, new int[] { 0 }), //
        new ProjectNode(bazFilter2, new int[] { 0 }) //
    ), //
        optimizer.apply(new ProjectNode(new UnionNode(//
        new ProjectNode(bazFilter1, new int[] { 2, 0 }), //
        new ProjectNode(bazFilter2, new int[] { 2, 0 }) //
    ), new int[] { 1 }))
    );
  }

  @Test
  public void testPushDownFilterOptimizer3() {
    /*
    # 1793 sort_{[0]} arity 1
    # 1327 +-π_{0} arity 1
    # 9779   +-σ_{1 = "person"} arity 2
    # 1729     +-rec_1729 arity 2
    # 1235       +-π_{0, 1 = 2} arity 2
    # 1020       | +-σ_{1 = "type"} arity 3
    # 1522       |   +-$ cat facts.tsv arity 3
    # 6182       +-π_{0, 1 = 3} arity 2
    # 1370         +-⋈_{1=0} arity 4
    # 1870           +-δ_1870 arity 2
    # 1053           +-π_{0, 1 = 2} arity 2
    # 1269             +-σ_{1 = "subclassof"} arity 3
    # 1522               +-$ cat facts.tsv arity 3
    */

    Optimizer optimizer = new PushDownFilter();

    PlanNode baz = new BuiltinNode(new CompoundTerm("baz", args3));

    RecursionNode input = new RecursionNode(baz.project(new int[]{0, 2}));
    input.addRecursivePlan(input.getDelta().join(baz, new int[]{0}, new int[]{0}).project(new int[]{0, 1}));

    RecursionNode expected = new RecursionNode(baz.equalityFilter(2, "xyz").project(new int[]{0, 2}));
    expected.addRecursivePlan(expected.getDelta().join(baz, new int[]{0}, new int[]{0}).project(new int[]{0, 1}));

    assertEquals(
            expected
            ,
            optimizer.apply(input.equalityFilter(1, "xyz"))
    );

  }

  @Test
  public void testPushDownProject() {
    Optimizer optimizer = new PushDownProject();

    PlanNode baz = new BuiltinNode(new CompoundTerm("baz", args5));

    assertEquals(new JoinNode(//
            baz.project(new int[] { 1, 2, 3, 4 }), //
            baz.project(new int[] { 1, 2, 3 }), //
            new int[] { 3, 1 }, new int[] { 0, 2 })//
                .project(new int[] { 0, 2, 5 }), //
            optimizer.apply(new JoinNode(baz, baz, //
                new int[] { 4, 2 }, new int[] { 1, 3 })//
                    .project(new int[] { 1, 3, 7 })));
  }

  @Test
  public void testMaterialization() {
    Optimizer optimizer = new Materialize();
    PlanNode baz = new BuiltinNode(new CompoundTerm("bar", args3));

    MaterializationNode.Builder b = new MaterializationNode.Builder(2);
    assertEquals(
        b.build(new UnionNode(//
            b.getReuseNode(), //
            b.getReuseNode().equalityFilter(0, 1)), //
            baz.project(new int[] { 1, 2 }), 2)
        ,
        optimizer.apply(new UnionNode(//
            baz.project(new int[] { 1, 2 }), //
            baz.project(new int[] { 1, 2 }).equalityFilter(0, 1)))
        );
    
  }

  public static void assertEquals(PlanNode expected, PlanNode actual) {
    if (!Objects.equals(expected, actual)) {
      System.out.println("expected:");
      System.out.println(expected.toPrettyString());
      System.out.println("\nactual:");
      System.out.println(actual.toPrettyString());
    }
    Assert.assertEquals(expected, actual);
  }

}
