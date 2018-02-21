package common.plan.node;

import java.util.*;
import java.util.stream.Collectors;

public class BashNode implements PlanNode {

  private final int arity;

  private final String command;

  private final List<String> commandParts;

  private final List<PlanNode> children;

  public BashNode(String command, List<String> commandParts, Collection<PlanNode> children, int arity) {
    this.command = command;
    this.commandParts = commandParts;
    this.children = new ArrayList<>(children);
    this.arity = arity;
  }

  @Override
  public int getArity() {
    return arity;
  }

  public String getCommand() {
    return command;
  }

  public List<String> getCommandParts() {
    return commandParts;
  }

  @Override
  public String operatorString() {
    return "bash: " + commandParts;
  }

  @Override
  public List<PlanNode> children() {
    return children;
  }

  @Override
  public boolean equals(Object obj) {
    return equals(obj, Collections.emptyMap());
  }

  @Override
  public boolean equals(Object obj, Map<PlanNode, PlanNode> assumedEqualities) {
    if(this == obj) return true;
    if (obj.getClass() != this.getClass()) {
      return false;
    }
    
    BashNode node = (BashNode) obj;
    if (this.arity != node.arity || !this.commandParts.equals(node.commandParts) || children.size() != node.children.size()) return false;
    for (int i = 0; i < children.size(); i++) {
      if (!children.get(i).equals(node.children.get(i), assumedEqualities)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return arity + Objects.hash(commandParts, children);
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    List<PlanNode> newChildren = this.children.stream().map(pn -> pn.transform(fn, this)).collect(Collectors.toList());
    PlanNode newNode = this;
    if (!this.children.equals(newChildren)) {
      newNode = new BashNode(this.command, this.commandParts, newChildren, this.arity);
    }
    return fn.apply(this, newNode, originalParent);
  }

}
