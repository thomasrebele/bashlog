package common.plan.optimizer;

import common.plan.node.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CombineFilter implements Optimizer {

  private boolean condenseNonUnionFilter = false;

  public CombineFilter(boolean condenseNonUnionFilter) {
    this.condenseNonUnionFilter = condenseNonUnionFilter;
  }

  @Override
  public PlanNode apply(PlanNode t) {
    return t.transform((node) -> {
      if (node.getClass() == UnionNode.class) {
        UnionNode u = (UnionNode) node;

        Map<PlanNode, Set<PlanNode>> innerToFiltered = new HashMap<>();
        for (PlanNode c : u.children()) {
          innerToFiltered.computeIfAbsent(getInnerTable(c), k -> new HashSet<>()).add(c);
        }

        return innerToFiltered.entrySet().stream().map(e -> {
          boolean isMultiFilter = e.getValue().size() > 1;
          if (condenseNonUnionFilter && !isMultiFilter && e.getValue().size() == 1) {
            int depth = getFilterDepth(e.getValue().iterator().next());
            isMultiFilter = depth > 1;
          }
          return isMultiFilter ? new MultiFilterNode(e.getValue(), e.getKey(), u.getArity()) : e.getValue().iterator().next();
        }).reduce(PlanNode.empty(node.getArity()), PlanNode::union);
      }

      return node;
    });
  }

  /** Descend into project and filter */
  public static PlanNode getInnerTable(PlanNode p) {
    if (p instanceof ProjectNode || p instanceof VariableEqualityFilterNode || p instanceof ConstantEqualityFilterNode) {
      return getInnerTable(p.children().get(0));
    }
    return p;
  }

  /** Descend into project and filter */
  public static int getFilterDepth(PlanNode parent) {
    if (parent instanceof ProjectNode || parent instanceof VariableEqualityFilterNode || parent instanceof ConstantEqualityFilterNode) {
      return 1 + getFilterDepth(parent.children().get(0));
    }
    return 0;
  }

}
