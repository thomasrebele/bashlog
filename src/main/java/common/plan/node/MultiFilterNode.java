package common.plan.node;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MultiFilterNode implements PlanNode {

  protected final PlanNode innerPlan;

  protected final Set<PlanNode> children;

  protected final int arity;

  protected final String operatorString;

  protected final PlanNode placeholder;

  public MultiFilterNode(Set<PlanNode> children, PlanNode innerPlan, int arity) {
    this.arity = arity;
    this.innerPlan = innerPlan;

    placeholder = new PlaceholderNode(this, "inner_plan", innerPlan.getArity());

    this.children = children.stream().map(c -> c.replace(innerPlan, placeholder)).collect(Collectors.toSet());
    operatorString = this.children.stream().map(PlanNode::toString).collect(Collectors.joining(", "));
    children.stream().filter(n -> !n.contains(innerPlan)).forEach(child -> {
      throw new IllegalArgumentException("child doesn't contain inner plan: " + child.toPrettyString());
    });
  }

  public MultiFilterNode(Set<PlanNode> children, PlanNode innerPlan, PlanNode placeholder, int arity) {
    this.children = children;
    this.innerPlan = innerPlan;
    this.placeholder = placeholder;
    this.arity = arity;
    operatorString = this.children.stream().map(PlanNode::toString).collect(Collectors.joining(", "));
  }

  public PlanNode getInnerTable() {
    return innerPlan;
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
    return Arrays.asList(innerPlan);
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
    return fn.apply(this, transform(children, innerPlan.transform(fn, this)), originalParent);
  }

  protected MultiFilterNode transform(Set<PlanNode> children, PlanNode innerPlan) {
    return new MultiFilterNode(children, innerPlan, placeholder, arity);
  }

}
