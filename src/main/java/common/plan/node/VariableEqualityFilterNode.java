package common.plan.node;

import java.util.Collections;
import java.util.List;

public class VariableEqualityFilterNode implements EqualityFilterNode {

  private final PlanNode table;

  private final int field1;

  private final int field2;

  public VariableEqualityFilterNode(PlanNode table, int field1, int field2) {
    if (field1 >= table.getArity() || field2 >= table.getArity()) {
      throw new IllegalArgumentException("The table has arity " + table.getArity() + " and the fields have ids " + field1 + " and " + field2);
    }
    this.table = table;
    this.field1 = Math.min(field1, field2);
    this.field2 = Math.max(field1, field2);
  }

  public PlanNode getTable() {
    return table;
  }

  public int getField1() {
    return field1;
  }

  public int getField2() {
    return field2;
  }

  @Override
  public int getArity() {
    return table.getArity();
  }

  @Override
  public String toString() {
    return operatorString() + "(" + table.toString() + ")";
  }

  @Override
  public String operatorString() {
    return "σ_{" + field1 + " = " + field2 + "}";
  }

  @Override
  public List<PlanNode> children() {
    return Collections.singletonList(table);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    VariableEqualityFilterNode node = (VariableEqualityFilterNode) obj;
    return table.equals(node.table) && field1 == node.field1 && field2 == node.field2;
  }

  @Override
  public int hashCode() {
    return "selection".hashCode() ^ table.hashCode() ^ (field1 + field2);
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    return fn.apply(this, new VariableEqualityFilterNode(table.transform(fn, this), field1, field2), originalParent);
  }
}
