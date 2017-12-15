package common.plan.node;

/** Filter for equality of two columns, or one column and a constant. */
public interface EqualityFilterNode extends PlanNode {
  PlanNode getTable();
}
