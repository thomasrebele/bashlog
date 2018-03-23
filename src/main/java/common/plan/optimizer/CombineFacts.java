package common.plan.optimizer;

import common.plan.node.FactNode;
import common.plan.node.PlanNode;
import common.plan.node.UnionNode;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Combine a union of selections/projections on one subplan to a single node
 */
public class CombineFacts implements Optimizer {

  @Override
  public PlanNode apply(PlanNode t) {
    return t.transform((old, node, parent) -> {
      if (node.getClass() == UnionNode.class) {
        UnionNode u = (UnionNode) node;

        // collect facts
        List<FactNode> facts = u.children().stream().filter(c -> c instanceof FactNode).map(c -> (FactNode) c).collect(Collectors.toList());

        if (facts.size() > 1) {
          FactNode fn = new FactNode(facts);
          Set<PlanNode> others = u.children().stream().filter(c -> !(c instanceof FactNode)).collect(Collectors.toSet());
          return fn.union(others);
        }

        return u;
      }

      return node;
    });
  }

}
