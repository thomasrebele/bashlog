package common.plan.optimizer;

import common.plan.node.PlanNode;
import common.plan.node.UnionNode;

public class PlanValidator implements Optimizer {

  /**
   * Checks whether plan is simplified
   */
  @Override
  public PlanNode apply(PlanNode node) {
    return node.transform((n) -> {
      if (n instanceof UnionNode) {
        check((UnionNode) n);
      }
      return n;
    });
  }

  private void check(UnionNode n) {
    for (PlanNode c : n.children()) {
      if (c instanceof UnionNode) {
        throw new IllegalStateException("union may not have a union as child");
      }
    }
  }

}
