package common.plan.node;

import common.Tools;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class MaterializationNode implements PlanNode {

  private final PlanNode mainPlan;

  private final PlanNode reusedPlan;

  private final PlanNode reuseNode;

  /** see {@link #getReuseCount()} */
  private final int reuseCount;

  public static class Builder {
  
    PlaceholderNode.Builder reuseNodeBuilder;

    public Builder(int arity) {
      reuseNodeBuilder = new PlaceholderNode.Builder("mat_{building}", arity);//new ReuseNode(null, arity);
    }

    public PlanNode getReuseNode() {
      return reuseNodeBuilder.preview();
    }
  
    public MaterializationNode build(PlanNode mainPlan, PlanNode reusedPlan, int reuseCount) {
      if (reuseNodeBuilder == null) throw new IllegalStateException("already built!");
      MaterializationNode result = new MaterializationNode(mainPlan, reusedPlan, reuseNodeBuilder, null, reuseCount);
      reuseNodeBuilder.build(result, result.operatorString().replaceAll("mat", "reuse") + " (reusing " + result.hash() + ")");
      return result;
    }
    
  }

  /** Use builder if possible */
  protected MaterializationNode(PlanNode mainPlan, PlanNode reusedPlan, PlaceholderNode.Builder builder, PlanNode reuseNode, int reuseCount) {
    // first initialize reuse node!
    if (builder != null) {
      builder.setParent(this);
      reuseNode = builder.preview();
    }

    if (!(reuseNode instanceof PlaceholderNode)) {
      throw new IllegalArgumentException();
    }
    this.reuseNode = builder != null ? reuseNode : new PlaceholderNode(this, reuseNode.operatorString(), reusedPlan.getArity());
    this.reusedPlan = reusedPlan;
    if (!mainPlan.contains(reuseNode)) {
      throw new IllegalArgumentException("incorrect reuse node specified:" + reuseNode.toPrettyString() + "\n" + mainPlan.toPrettyString());
    }
    this.mainPlan = builder != null ? mainPlan : mainPlan.replace(reuseNode, this.reuseNode);
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
    PlanNode newMainPlan = mainPlan.transform(fn, this);
    PlanNode newReusedPlan = reusedPlan.transform(fn, this);
    PlanNode newNode = mainPlan.equals(newMainPlan) && reusedPlan.equals(newReusedPlan) ? this
            : new MaterializationNode(newMainPlan, newReusedPlan, null, reuseNode, reuseCount);
    return fn.apply(this, newNode, originalParent);
  }

  @Override
  public boolean equals(Object obj) {
    return equals(obj, Collections.emptyMap());
  }

  @Override
  public boolean equals(Object obj,  Map<PlanNode,PlanNode> assumedEqualities) {
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    MaterializationNode node = (MaterializationNode) obj;
    assumedEqualities = Tools.with(assumedEqualities, reuseNode, node.reuseNode);
    return mainPlan.equals(node.mainPlan, assumedEqualities) && reusedPlan.equals(node.reusedPlan, assumedEqualities);
  }
}
