package common.plan.node;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PlaceholderNode implements PlanNode {

  protected PlanNode parent;

  protected String operatorString;

  protected final Integer arity;

  public static class Builder {

    private PlaceholderNode node;

    public Builder(String operatorString, Integer arity) {
      node = new PlaceholderNode(null, operatorString, arity);
    }

    public PlaceholderNode preview() {
      return node;
    }

    public void setParent(PlanNode parent) {
      node.parent = parent;
    }

    public PlaceholderNode build(PlanNode parent, String operatorString) {
      if (node == null) throw new IllegalStateException("already built!");
      try {
        node.parent = parent;
        node.operatorString = operatorString;
        return node;
      } finally {
        node = null;
      }
    }
  }

  public PlaceholderNode(PlanNode parent, String operatorString) {
    this.parent = parent;
    this.operatorString = operatorString;
    arity = null;
  }

  public PlaceholderNode(PlanNode parent, String operatorString, Integer arity) {
    this.parent = parent;
    this.operatorString = operatorString;
    this.arity = arity;
  }

  public PlanNode getParent() {
    return parent;
  }

  @Override
  public String toString() {
    return operatorString;
  }

  @Override
  public boolean equals(Object obj) {
    return equals(obj, Collections.emptyMap());
  }

  @Override
  public boolean equals(Object obj,  Map<PlanNode,PlanNode> assumedEqualities) {
    return assumedEqualities.getOrDefault(this, this) == obj;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int getArity() {
    return arity == null ? parent.getArity() : arity;
  }

  @Override
  public List<PlanNode> children() {
    return Collections.emptyList();
  }

  @Override
  public String operatorString() {
    return operatorString;
  }

}