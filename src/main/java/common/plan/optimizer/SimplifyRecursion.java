package common.plan.optimizer;

import common.plan.node.PlaceholderNode;
import common.plan.node.PlanNode;
import common.plan.node.RecursionNode;
import common.plan.node.UnionNode;

import java.util.Collections;

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
}
