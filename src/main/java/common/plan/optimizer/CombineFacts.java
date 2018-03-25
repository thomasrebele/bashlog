package common.plan.optimizer;

import java.util.*;
import java.util.stream.Collectors;

import common.plan.node.*;

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
          Set<PlanNode> others = u.children().stream().filter(c -> !(c instanceof FactNode)).map(c -> (FactNode) c).collect(Collectors.toSet());
          return fn.union(others);
        }

        return u;
      }

      return node;
    });
  }

}
