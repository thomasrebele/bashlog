package common.plan.node;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MultiFilterNode implements PlanNode {

  private final PlanNode table;

  protected final Set<PlanNode> children;

  protected final int arity;

  protected final String operatorString;

  private final PlanNode placeholder;

  public MultiFilterNode(Set<PlanNode> children, PlanNode table, int arity) {
    this.arity = arity;
    this.table = table;

    placeholder = new PlaceholderNode(this, "inner_plan", table.getArity());

    this.children = children.stream().map(c -> c.replace(table, placeholder)).collect(Collectors.toSet());
    operatorString = this.children.stream().map(PlanNode::toString).collect(Collectors.joining(", "));
    children.stream().filter(n -> !n.contains(table)).forEach(child -> {
      throw new IllegalArgumentException("child doesn't contain inner plan: " + child.toPrettyString());
    });
  }

  public MultiFilterNode(Set<PlanNode> children, PlanNode table, PlanNode placeholder, int arity) {
    this.children = children;
    this.table = table;
    this.placeholder = placeholder;
    this.arity = arity;
    operatorString = this.children.stream().map(PlanNode::toString).collect(Collectors.joining(", "));
  }

  public PlanNode getTable() {
    return table;
  }

  public Set<PlanNode> getFilter() {
    return children;
  }

  // TODO: toString / operatorString
  public String operatorString() {
    return "multi_filter (" + operatorString + ")";
  }

  @Override
  public int getArity() {
    return arity;
  }

  @Override
  public List<PlanNode> children() {
    return Arrays.asList(table);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    MultiFilterNode node = (MultiFilterNode) obj;
    return children.equals(node.children);
  }

  @Override
  public int hashCode() {
    return children.hashCode();
  }

  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    PlanNode newTable = table.transform(fn, this);
    PlanNode newNode = newTable.equals(table) ? this : new MultiFilterNode(children, newTable, placeholder, arity);
    return fn.apply(this, newNode, originalParent);
  }
}
