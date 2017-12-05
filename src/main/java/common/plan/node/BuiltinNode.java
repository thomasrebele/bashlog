package common.plan.node;

import java.util.Collections;
import java.util.List;

import common.parser.CompoundTerm;
import common.parser.Constant;

public class BuiltinNode implements PlanNode {

  public final CompoundTerm compoundTerm;

  public BuiltinNode(CompoundTerm t) {
    this.compoundTerm = t;
  }

  @Override
  public int getArity() {
    return (int) compoundTerm.getVariables().count();
  }

  @Override
  public String toString() {
    return operatorString();
  }

  @Override
  public String operatorString() {
    if ("bash_command".equals(compoundTerm.name)) {
      return "$ " + ((Constant<?>) compoundTerm.args[0]).getValue();
    }
    return "builtin:" + compoundTerm;
  }

  @Override
  public List<PlanNode> children() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof BuiltinNode && compoundTerm.equals(((BuiltinNode) obj).compoundTerm);
  }

  @Override
  public int hashCode() {
    return compoundTerm.hashCode();
  }
}
