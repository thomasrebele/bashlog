package bashlog.plan;

import java.util.Collections;
import java.util.List;

import common.plan.PlanNode;

public class TSVFileNode implements PlanNode {

  final String path;

  final int arity;

  public TSVFileNode(String path, int arity) {
    this.path = path;
    this.arity = arity;
  }

  @Override
  public int getArity() {
    return arity;
  }

  @Override
  public String operatorString() {
    return "file: " + path;
  }

  @Override
  public List<PlanNode> args() {
    return Collections.emptyList();
  }

  public String getPath() {
    return path;
  }

    @Override
  public boolean equals(Object obj) {
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    TSVFileNode node = (TSVFileNode) obj;
    return this.path.equals(node.path) && this.arity == node.arity;
  }

  @Override
  public int hashCode() {
    return path.hashCode() ^ this.arity;
  }
  
}
