package common.plan.optimizer;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import common.plan.node.*;

/**
 * Simplifies plan by applying equivalence transformations like
 * - project_(field=field){table} =&gt; table
 * - join {empty} {table} =&gt; empty
 */
public class SimplifyRecursion implements Optimizer {

  public PlanNode apply(PlanNode node) {
    return node.transform((n) -> {
      if (n instanceof RecursionNode) {
        return simplify((RecursionNode) n);
      } else {
        return n;
      }
    });
  }

  private PlanNode simplify(RecursionNode node) {
    PlanNode exitPlan = node.getExitPlan();
    PlanNode recursivePlan = PlanNode.empty(node.getArity());

    //We see if the recursion is a union with some not recursive parts and move them to exitPlan
    for (PlanNode child : (node.getRecursivePlan() instanceof UnionNode)
            ? node.getRecursivePlan().children()
            : Collections.singleton(node.getRecursivePlan())) {
      if (child.contains(node.getFull()) || child.contains(node.getDelta())) {
        recursivePlan = recursivePlan.union(child);
      } else {
        exitPlan = exitPlan.union(child);
      }
    }

    //We make sure that exit and recursive plans are simplified
    exitPlan = apply(exitPlan);
    recursivePlan = apply(recursivePlan);

    // search for unnecessary delta/full nodes
    Set<List<PlanNode>> remove = superfluous(recursivePlan);

    //No recursion or recursion with just Full or delta
    if (recursivePlan.isEmpty() || recursivePlan instanceof PlaceholderNode) {
      return exitPlan;
    }

    //Empty recursion
    PlanNode unrolledOncePlan = apply(recursivePlan
            .replace(node.getDelta(), exitPlan)
            .replace(node.getFull(), exitPlan));
    if (unrolledOncePlan.isEmpty()) {
      //The loop do not adds data to the exit plan
      return exitPlan;
    }

    return node;
  }

  /**
   * 
   * @param recursivePlan
   * @return set of paths to placeholder
   */
  private Set<List<PlanNode>> superfluous(PlanNode recursivePlan) {
    Set<List<PlanNode>> result = new HashSet<>();


    return result;
  }

  public static void main(String[] args) {
    RecursionNode.Builder builder = new RecursionNode.Builder(new FactNode("abc", "def"));
    builder.addRecursivePlan(builder.getDelta().union(builder.getFull()));
    PlanNode p = builder.build();


    System.out.println(p.toPrettyString());
    System.out.println("---");
    p = new SimplifyRecursion().apply(p);
    System.out.println(p.toPrettyString());
  }

}
