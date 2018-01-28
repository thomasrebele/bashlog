package common.plan.optimizer;

import java.util.*;

import common.plan.node.*;

/**
 * Combine a union of selections/projections on one subplan to a single node
 */
public class CombineFilter implements Optimizer {

  private boolean condenseNonUnionFilter = false;

  public CombineFilter(boolean condenseNonUnionFilter) {
    this.condenseNonUnionFilter = condenseNonUnionFilter;
  }

  @Override
  public PlanNode apply(PlanNode t) {
    return t.transform((old, node, parent) -> {
      if (node.getClass() == UnionNode.class) {
        UnionNode u = (UnionNode) node;

        // collect plans within projections/selections
        Map<PlanNode, Set<PlanNode>> innerToFiltered = new HashMap<>();
        for (PlanNode c : u.children()) {
          innerToFiltered.computeIfAbsent(getInnerTable(c), k -> new HashSet<>()).add(c);
        }

        // replace by a multi filter node
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
