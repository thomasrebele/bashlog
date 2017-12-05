package common.plan.node;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class PlaceholderNode implements PlanNode {

  protected final PlanNode parent;

  protected final String operatorString;

  protected final Integer arity;

  public PlaceholderNode(PlanNode parent, String operatorString) {
    this.parent = parent;
    this.operatorString = operatorString;
    arity = null;
    }

  public PlaceholderNode(PlanNode parent, String operatorString, int arity) {
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

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PlaceholderNode && ((PlaceholderNode) obj).parent == parent && Objects.equals(((PlaceholderNode) obj).operatorString, operatorString)
        && ((PlaceholderNode) obj).arity == arity;
  }
}