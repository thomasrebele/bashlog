package common.plan.node;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConstantEqualityFilterNode implements EqualityFilterNode {

  private final PlanNode table;

  private final int field;

  private final Comparable<?> value;

  ConstantEqualityFilterNode(PlanNode table, int field, Comparable<?> value) {
    if (field >= table.getArity()) {
      throw new IllegalArgumentException("The table has arity " + table.getArity() + " and the field has id " + field);
    }
    this.table = table;
    this.field = field;
    this.value = value;
  }

  public PlanNode getTable() {
    return table;
  }

  public int getField() {
    return field;
  }

  public Comparable<?> getValue() {
    return value;
  }

  @Override
  public int getArity() {
    return table.getArity();
  }

  @Override
  public String toString() {
    return operatorString() + "(" + table + ")";
  }

  @Override
  public String operatorString() {
    return "σ_{" + field + " = \"" + value.toString() + "\"}";
  }

  @Override
  public List<PlanNode> children() {
    return Collections.singletonList(table);
  }

  @Override
  public boolean equals(Object obj) {
    return equals(obj, Collections.emptyMap());
  }

  @Override
  public boolean equals(Object obj, Map<PlanNode, PlanNode> assumedEqualities) {
    if (this == obj) return true;
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    ConstantEqualityFilterNode node = (ConstantEqualityFilterNode) obj;
    return field == node.field && value.equals(node.value) && table.equals(node.table, assumedEqualities);
  }

  @Override
  public int hashCode() {
    return "selection".hashCode() ^ table.hashCode() ^ value.hashCode();
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    PlanNode newTable = table.transform(fn, this);
    PlanNode newNode = newTable.equals(table) ? this : newTable.equalityFilter(field, value);
    return fn.apply(this, newNode, originalParent);
  }
}
