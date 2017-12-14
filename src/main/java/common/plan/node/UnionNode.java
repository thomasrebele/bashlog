package common.plan.node;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class UnionNode implements PlanNode {

  protected final Set<PlanNode> children;

  protected final int arity;

  public UnionNode(Set<PlanNode> children, int arity) {
    this.children = children;
    this.arity = arity;
  }

  UnionNode(int arity) {
    this(Collections.emptySet(), arity);
  }

  public UnionNode(PlanNode... children) {
    this(new HashSet<PlanNode>(Arrays.asList(children)));
  }

  public UnionNode(Set<PlanNode> children) {
    if (children.size() == 0) {
      throw new IllegalArgumentException("Please set the arity of this empty union");
    }

    int arity = -1;
    for (PlanNode child : children) {
      if (arity == -1) {
        arity = child.getArity();
      } else if (child.getArity() != arity) {
        throw new IllegalArgumentException("All terms of the union should have the same arity");
      }
    }
    this.arity = arity;
    this.children = children;
  }

  public UnionNode(Stream<PlanNode> children) {
    this(children.collect(Collectors.toSet()));
  }

  public Collection<PlanNode> getChildren() {
    return children;
  }

  @Override
  public int getArity() {
    return arity;
  }

  @Override
  public boolean isEmpty() {
    return children.isEmpty();
  }


  @Override
  public String toString() {
    return children.stream().map(Object::toString).collect(Collectors.joining(" " + operatorString() + " "));
  }

  @Override
  public String operatorString() {
    return "∪";
  }

  @Override
  public List<PlanNode> children() {
    return new ArrayList<>(children);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    UnionNode node = (UnionNode) obj;
    return children.equals(node.children);
  }

  @Override
  public int hashCode() {
    return children.hashCode();
  }

  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    Set<PlanNode> newChildren = children.stream().map(child -> child.transform(fn, this)).collect(Collectors.toSet());
    PlanNode newNode = newChildren.equals(children) ? this : new UnionNode(newChildren, this.arity);
    return fn.apply(this, newNode, originalParent);
  }
}
