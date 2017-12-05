package common.plan.node;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;


public class MaterializationNode implements PlanNode {

  final PlanNode mainPlan;

  final PlanNode reusedPlan;

  final PlanNode reuseNode;

  /** see {@link #getReuseCount()} */
  final int reuseCount;

  public static class Builder {
  
    ReuseNode reuseNode;

    public Builder(int arity) {
      reuseNode = new ReuseNode(null, arity);
    }

    public PlanNode getReuseNode() {
      return reuseNode;
    }
  
    public MaterializationNode build(PlanNode mainPlan, PlanNode reusedPlan, int reuseCount) {
      if (reuseNode == null) throw new IllegalStateException("already built!");
      MaterializationNode result = new MaterializationNode(mainPlan, reusedPlan, reuseNode, reuseCount, true);
      reuseNode.matNode = result;
      reuseNode = null;
      return result;
    }
    
  }

  /** Use builder if possible */
  protected MaterializationNode(PlanNode mainPlan, PlanNode reusedPlan, PlanNode reuseNode, int reuseCount, boolean building) {
    if (!(reuseNode instanceof ReuseNode)) {
      throw new IllegalArgumentException();
    }
    // first initialize reuse node!
    if (building) ((ReuseNode) reuseNode).matNode = this;
    this.reuseNode = building ? reuseNode : new ReuseNode(this, reusedPlan.getArity());
    this.reusedPlan = reusedPlan;
    if (!mainPlan.contains(reuseNode)) {
      throw new IllegalArgumentException("incorrect reuse node specified:" + reuseNode.toPrettyString() + "\n" + mainPlan.toPrettyString());
    }
    this.mainPlan = building ? mainPlan : mainPlan.replace(reuseNode, this.reuseNode);
    this.reuseCount = reuseCount;
  }

  public PlanNode getReusedPlan() {
    return reusedPlan;
  }

  public PlanNode getMainPlan() {
    return mainPlan;
  }

  public PlanNode getReuseNode() {
    return reuseNode;
  }

  /** How often the plan is reused. Infinity is denoted by -1 */
  public int getReuseCount() {
    return reuseCount;
  }

  @Override
  public int getArity() {
    return mainPlan == null ? 0 : mainPlan.getArity();
  }

  @Override
  public String operatorString() {
    return "mat_" + hash();
  }

  @Override
  public List<PlanNode> children() {
    return Arrays.asList(reusedPlan, mainPlan);
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    PlanNode transformed = mainPlan.transform(fn, this);
    return fn.apply(this, transform(transformed, reusedPlan.transform(fn, this)), originalParent);
  }

  public MaterializationNode transform(PlanNode mainPlan, PlanNode reusedPlan) {
    return new MaterializationNode(mainPlan, reusedPlan, reuseNode, reuseCount, false);
  }

  public static class ReuseNode implements PlanNode {

    MaterializationNode matNode;

    final int arity;

    public ReuseNode(MaterializationNode materializationNode, int arity) {
      this.matNode = materializationNode;
      this.arity = arity;
    }

    @Override
    public int getArity() {
      return arity;
    }

    @Override
    public String operatorString() {
      if (matNode == null) {
        return "mat_{building}";
      } else {
        return matNode.operatorString().replaceAll("mat", "reuse") + " (reusing " + matNode.hash() + ")";
      }
    }

    @Override
    public List<PlanNode> children() {
      return Arrays.asList();
    }

    public MaterializationNode getMaterializeNode() {
      return matNode;
    }

    @Override
    public int hashCode() {
      // during building, equals and hash directly on ReuseNode
      return 1;
    }
    
    @Override
    public boolean equals(Object obj) {
      // during building, equals and hash directly on ReuseNode
      if (obj.getClass() != ReuseNode.class) return false;
      return matNode == null ? super.equals(obj) : Objects.equals(matNode, ((ReuseNode) obj).matNode);
    }
  }


}
