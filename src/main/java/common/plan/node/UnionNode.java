package common.plan.node;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import common.Tools;

public class UnionNode implements PlanNode {

  protected final Set<PlanNode> children;

  protected final int arity;

  private UnionNode(Set<PlanNode> children, int arity) {
    this.children = children;
    this.arity = arity;
  }

  UnionNode(int arity) {
    this(Collections.emptySet(), arity);
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
    return equals(obj, Collections.emptyMap());
  }

  @Override
  public boolean equals(Object obj, Map<PlanNode, PlanNode> assumedEqualities) {
    if (this == obj) return true;
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    UnionNode node = (UnionNode) obj;
    return children.size() == node.children.size()
        && children.stream().allMatch(child -> node.children.stream().anyMatch(other -> child.equals(other, assumedEqualities)));
  }

  @Override
  public int hashCode() {
    return children.hashCode();
  }

  public PlanNode transform(TransformFn fn, List<PlanNode> originalPath) {
    try {
      Tools.addLast(originalPath, this);
      Set<PlanNode> newChildren = children.stream()//
          .map(child -> child.transform(fn, originalPath))//
          .flatMap(child -> (Stream<PlanNode>) ((child instanceof UnionNode) ? ((UnionNode) child).children().stream() : Stream.of(child)))//
          .collect(Collectors.toSet());
      PlanNode newNode = newChildren.equals(children) ? this : new UnionNode(newChildren, this.arity);
      return fn.apply(this, newNode, originalPath);
    } finally {
      Tools.removeLast(originalPath);
    }
  }
}
