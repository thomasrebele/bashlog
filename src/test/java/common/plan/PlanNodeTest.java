package common.plan;

import java.util.Objects;

import org.junit.Assert;
import org.junit.Test;

import common.parser.*;
import common.plan.node.*;
import common.plan.optimizer.*;

public class PlanNodeTest {

  private TermList args1 = new TermList(new Variable("X"));

  private TermList args2 = new TermList(new Variable("X"), new Variable("Y"));

  private TermList args3 = new TermList(new Variable("X"), new Variable("Y"), new Variable("Z"));

  private TermList args4 = new TermList(new Variable("X"), new Variable("Y"), new Variable("Z"), new Variable("W"));

  private TermList args5 = new TermList(new Variable("X"), new Variable("Y"), new Variable("Z"), new Variable("W"), new Variable("V"));

  @Test
  public void testPlanNode() {
    PlanNode foo = new BuiltinNode(new CompoundTerm("foo", args2));
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args2));
    Assert.assertEquals(foo, foo);
    Assert.assertNotEquals(foo, bar);
    Assert.assertTrue(foo.equalityFilter(1, 1).contains(foo));
    Assert.assertFalse(foo.equalityFilter( 1, 1).contains(bar));
    Assert.assertEquals(
            foo.equalityFilter(1, 1),
            bar.equalityFilter(1, 1).replace(bar, foo)
    );
  }

  @Test
  public void testSimplifier() {
    PlanNode foo = new BuiltinNode(new CompoundTerm("foo", args2));
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args2));
    PlanNode baz = new BuiltinNode(new CompoundTerm("bar", args4));
    Assert.assertEquals(
            foo.union(bar),
            foo.union(bar.union(PlanNode.empty(2)))
    );
    Assert.assertEquals(
            foo,
            foo.union(foo.union(PlanNode.empty(2)))
    );

    Assert.assertEquals(
            foo,
            foo.project(new int[]{0, 1})
    );
    Assert.assertEquals(
            PlanNode.empty(2),
            PlanNode.empty(2).project(new int[]{0, 0})
    );
    Assert.assertEquals(
            PlanNode.empty(1),
            PlanNode.empty(2).project(new int[]{0})
    );
    Assert.assertEquals(
            baz.project(new int[]{0, 1, 3}),
            baz.project(new int[]{3, 0, 1}).project(new int[]{1, 2, 0})
    );
    Assert.assertEquals(
            baz.project(new int[]{0}),
            baz.project(new int[]{2, 0}).project(new int[]{1})
    );

    Assert.assertEquals(
            PlanNode.empty(4),
            foo.join(PlanNode.empty(2), new int[]{0}, new int[]{0})
    );
    Assert.assertEquals(
            foo.join(bar, new int[]{0}, new int[]{0}),
            foo.join(bar, new int[]{0}, new int[]{0})
    );

    Assert.assertEquals(
            foo,
            foo.antiJoin(PlanNode.empty(1), new int[]{0})
    );
    Assert.assertEquals(
            PlanNode.empty(2),
            PlanNode.empty(2).antiJoin(foo, new int[]{0, 1})
    );
  }


  @Test
  public void testSimplifyRecursion() {
    SimplifyRecursion simplifier = new SimplifyRecursion();
    PlanNode foo = new BuiltinNode(new CompoundTerm("foo", args2));
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args2));

    RecursionNode.Builder builder = new RecursionNode.Builder(foo.getArity());
    builder.addRecursivePlan(builder.getDelta().join(bar, new int[] { 0 }, new int[] { 0 }).project(new int[] { 0, 1 }));
    RecursionNode recursionNode = builder.build(foo);
    Assert.assertTrue(simplifier.apply(recursionNode) instanceof RecursionNode);

    builder = new RecursionNode.Builder(2);
    builder.addRecursivePlan(builder.getDelta());
    RecursionNode recursionNode2 = builder.build(PlanNode.empty(2));
    Assert.assertEquals(PlanNode.empty(2), simplifier.apply(recursionNode2));

    builder = new RecursionNode.Builder(foo.getArity());
    builder.addRecursivePlan(bar);
    RecursionNode recursionNode3 = builder.build(foo);
    Assert.assertEquals(foo.union(bar), simplifier.apply(recursionNode3));

    builder = new RecursionNode.Builder(2);
    builder.addRecursivePlan(foo.union(builder.getDelta()));
    RecursionNode recursionNode4 = builder.build(PlanNode.empty(2));
    Assert.assertEquals(foo, simplifier.apply(recursionNode4));
  }

  @Test
  public void testPushDownFilterOptimizer() {
    Optimizer optimizer = new PushDownFilterAndProject();
    PlanNode foo = new BuiltinNode(new CompoundTerm("foo", args2));
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args2));
    PlanNode fooFilter = foo.equalityFilter(0, "foo");
    PlanNode barFilter = bar.equalityFilter(0, "foo");

    Assert.assertEquals(
            fooFilter.union(barFilter),
            optimizer.apply(foo.union(bar).equalityFilter(0, "foo"))
    );

    Assert.assertEquals(
            fooFilter.project(new int[]{1, 0}),
            optimizer.apply(foo.project(new int[]{1, 0}).equalityFilter(1, "foo"))
    );
    Assert.assertEquals(
            foo.project(new int[]{0, -1}, new Comparable[]{null, "foo"}),
            optimizer.apply(foo.project(new int[]{0, -1}, new Comparable[]{null, "foo"}).equalityFilter(1, "foo"))
    );
    Assert.assertEquals(
            PlanNode.empty(2),
            optimizer.apply(foo.project(new int[]{0, -1}, new Comparable[]{null, "bar"}).equalityFilter(1, "foo"))
    );

    Assert.assertEquals(
            fooFilter.join(bar, new int[]{1}, new int[]{1}),
            optimizer.apply(foo.join(bar, new int[]{1}, new int[]{1}).equalityFilter(0, "foo"))
    );
    Assert.assertEquals(
            foo.join(barFilter, new int[]{1}, new int[]{1}),
            optimizer.apply(foo.join(bar, new int[]{1}, new int[]{1}).equalityFilter(2, "foo"))
    );
    Assert.assertEquals(
            fooFilter.join(barFilter, new int[]{0}, new int[]{0}),
            optimizer.apply(foo.join(bar, new int[]{0}, new int[]{0}).equalityFilter(0, "foo"))
    );
    Assert.assertEquals(
            fooFilter.join(barFilter, new int[]{0}, new int[]{0}),
            optimizer.apply(foo.join(bar, new int[]{0}, new int[]{0}).equalityFilter(2, "foo"))
    );
  }

  @Test
  public void testPushDownFilterOptimizer2() {
    Optimizer optimizer = new PushDownFilterAndProject();

    PlanNode baz = new BuiltinNode(new CompoundTerm("baz", args3));
    PlanNode bazFilter1 = baz.equalityFilter(1, "foo1");
    PlanNode bazFilter2 = baz.equalityFilter(1, "foo2");
    Assert.assertEquals(bazFilter1.project(new int[]{0}).union(
            bazFilter2.project(new int[]{0})
            ),
            optimizer.apply(
                    bazFilter1.project(new int[]{2, 0}).union(
                            bazFilter2.project(new int[]{2, 0})
                    ).project(new int[]{1}))
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

    Optimizer optimizer = new PushDownFilterAndProject();

    PlanNode baz = new BuiltinNode(new CompoundTerm("baz", args3));

    PlanNode exit = baz.project(new int[] { 0, 2 });
    RecursionNode.Builder builder = new RecursionNode.Builder(exit.getArity());
    builder.addRecursivePlan(builder.getDelta().join(baz, new int[] { 0 }, new int[] { 0 }).project(new int[] { 0, 1 }));
    RecursionNode input = builder.build(exit);

    exit = baz.equalityFilter(2, "xyz").project(new int[] { 0, 2 });
    builder = new RecursionNode.Builder(exit.getArity());
    builder.addRecursivePlan(builder.getDelta().join(baz.project(new int[] { 0 }), new int[] { 0 }, new int[] { 0 }).project(new int[] { 0, 1 }));
    RecursionNode expected = builder.build(exit);

    assertEquals(
            expected,
            optimizer.apply(input.equalityFilter(1, "xyz"))
    );
  }

  @Test
  public void testPushDownFilterAntiJoinBoth() {
    Optimizer optimizer = new PushDownFilterAndProject();
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args3));
    PlanNode baz = new BuiltinNode(new CompoundTerm("baz", args2));
    assertEquals(
           bar.equalityFilter(0, "foo").antiJoin(
                    baz.equalityFilter(1, "foo"),
                    new int[]{1, 0}
            ),
            optimizer.apply(bar.antiJoin(baz, new int[]{1, 0}).equalityFilter(0, "foo"))
    );
  }

  @Test
  public void testPushDownFilterAntiJoinLeft() {
    Optimizer optimizer = new PushDownFilterAndProject();
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args3));
    PlanNode baz = new BuiltinNode(new CompoundTerm("baz", args2));
    assertEquals(
            bar.equalityFilter(2, "foo").antiJoin(baz, new int[]{1, 0}),
            optimizer.apply(
                    bar.antiJoin(baz, new int[]{1, 0}).equalityFilter(2, "foo"))
    );
  }

  @Test
  public void testPushDownProjectJoin() {
    Optimizer optimizer = new PushDownFilterAndProject();

    PlanNode baz = new BuiltinNode(new CompoundTerm("baz", args5));

    assertEquals(baz.project(new int[]{1, 2, 3, 4}).join(
                    baz.project(new int[]{1, 2, 3}), //
                    new int[]{3, 1}, new int[]{0, 2}
            ).project(new int[]{0, 2, 5}), //
            optimizer.apply(
                   baz.join(baz, new int[]{4, 2}, new int[]{1, 3}).project(new int[]{1, 3, 7})
            )
    );
  }


  @Test
  public void testPushDownProjectAntiJoin() {
    Optimizer optimizer = new PushDownFilterAndProject();
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args3));
    PlanNode baz = new BuiltinNode(new CompoundTerm("baz", args2));
    assertEquals(
            bar.project(new int[]{1, 0}).antiJoin(
                    baz,
                    new int[]{0, 1}
            ),
            optimizer.apply(bar.antiJoin(baz, new int[]{1, 0}).project(new int[]{1, 0}
            ))
    );
  }

  @Test
  public void testMaterialization() {
    Optimizer optimizer = new Materialize();
    PlanNode baz = new BuiltinNode(new CompoundTerm("bar", args3));

    MaterializationNode.Builder b = new MaterializationNode.Builder(2);
    assertEquals(
            b.build(
                    b.getReuseNode().union(b.getReuseNode().equalityFilter(0, 1)), //
                    baz.project(new int[]{1, 2}), 2)
            ,
            optimizer.apply(baz.project(new int[]{1, 2}).union(
                    baz.project(new int[]{1, 2}).equalityFilter(0, 1)))
    );

  }

  @Test
  public void testPushDownJoin() {
    Optimizer optimizer = new PushDownJoin();
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args4));
    // push left
    assertEquals(//
        bar.join(bar, new int[] { 2 }, new int[] { 2 }).project(new int[] { 2, 1, 4, 5, 6, 7 }), //
        optimizer.apply(
            bar.project(new int[] { 2, 1 }).join(bar, new int[] { 0 }, new int[] { 2 })
            )
        );

    // push right
    assertEquals(//
        bar.join(bar, new int[] { 3 }, new int[] { 2 }).project(new int[] { 0, 1, 2, 3, 6, 5 }), //
        optimizer.apply(bar.join(bar.project(new int[] { 2, 1 }), new int[] { 3 }, new int[] { 0 })));
  }

  @Test
  public void testJoinReorder() {
    PlanNode foo = new BuiltinNode(new CompoundTerm("foo", args2));
    PlanNode bar = new BuiltinNode(new CompoundTerm("bar", args1));
    
    Optimizer optimizer = new ReorderJoinLinear();
    assertEquals(
        foo.join(bar, new int[] { 0 }, new int[] { 0 }).join(bar, new int[] { 1 }, new int[] { 0 }).project(new int[] { 2, 3, 0, 1 }), //
        optimizer.apply(bar.join(bar, new int[] {}, new int[] {}).join(foo, new int[] { 0, 1 }, new int[] { 0, 1 }))
        );
  }

  private static void assertEquals(PlanNode expected, PlanNode actual) {
    if (!Objects.equals(expected, actual)) {
      System.out.println("expected:");
      System.out.println(expected.toPrettyString());
      System.out.println("\nactual:");
      System.out.println(actual.toPrettyString());
    }
    Assert.assertEquals(expected, actual);
  }

}
