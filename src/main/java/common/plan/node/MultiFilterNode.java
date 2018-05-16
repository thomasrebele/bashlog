package common.plan.node;

import common.Tools;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultiFilterNode implements PlanNode {

  private final PlanNode table;

  protected final Set<PlanNode> children;

  protected final int arity;

  protected final String operatorString;

  private final PlaceholderNode placeholder;

  public MultiFilterNode(Set<PlanNode> children, PlanNode table, int arity) {
    this.arity = arity;
    this.table = table;

    placeholder = new PlaceholderNode("inner_plan", table.getArity());

    this.children = children.stream().map(c -> c.replace(table, placeholder)).collect(Collectors.toSet());
    operatorString = this.children.stream().map(PlanNode::toString).collect(Collectors.joining(", "));
    children.stream().filter(n -> !n.contains(table)).forEach(child -> {
      throw new IllegalArgumentException("child doesn't contain inner plan: " + child.toPrettyString());
    });
  }

  public MultiFilterNode(Set<PlanNode> children, PlanNode table, PlaceholderNode placeholder, int arity) {
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
    return "multi_filter_" + hash();
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
  public List<PlanNode> childrenForPrettyString() {
    return Stream.concat(children.stream(), Stream.of(table)).collect(Collectors.toList());
  }

  @Override
  public List<PlaceholderNode> placeholders() {
    return Arrays.asList(placeholder);
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
    MultiFilterNode node = (MultiFilterNode) obj;
    Map<PlanNode, PlanNode> newAssumedEqualities = Tools.with(assumedEqualities, placeholder, node.placeholder);
    return table.equals(node.table, newAssumedEqualities) && children.size() == node.children.size()
        && children.stream().allMatch(child -> node.children.stream().anyMatch(other -> other.equals(child, newAssumedEqualities)));
  }

  @Override
  public int hashCode() {
    return children.hashCode();
  }

  public PlanNode transform(TransformFn fn, List<PlanNode> originalPath) {
    try {
      Tools.addLast(originalPath, this);
      PlanNode newTable = table.transform(fn, originalPath);
      PlanNode newNode = newTable.equals(table) ? this : new MultiFilterNode(children, newTable, placeholder, arity);
      return fn.apply(this, newNode, originalPath);
    } finally {
      Tools.removeLast(originalPath);
    }
  }
}
