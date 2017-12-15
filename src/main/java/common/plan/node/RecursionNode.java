package common.plan.node;

import common.Tools;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** 
 * Represents a plan node, that has itself as subplan.
 * 

 */
public class RecursionNode implements PlanNode {

  private final PlanNode exitPlan;

  private PlanNode recursivePlan;

  protected final PlanNode deltaNode = new PlaceholderNode(this, "delta");

  protected final PlanNode fullNode = new PlaceholderNode(this, "full");

  /**
   * Constructs a recursion.
   * 
   * Usecase example:
   * r1: foo(x,y) <- bar(x,y)
   * r2: foo(x,z) <- foo(x,y), bar(y,z)
   * <p>
   * We will have for exit plan the plan for r1 (input of the loop) and we will add the recursive plan for r2 where
   * foo call is replaced by the delta node (retrieved by getDelta()) 
   */
  public RecursionNode(PlanNode exitPlan) {
    this.exitPlan = exitPlan;
    recursivePlan = PlanNode.empty(exitPlan.getArity());
  }

  /** Use exit plan and recursive plan. The delta is the one in the recursive plan. It will be replaced by a new delta. */
  protected RecursionNode(PlanNode exitPlan, PlanNode recursivePlan, PlanNode delta, PlanNode full) {
    if (exitPlan.getArity() != recursivePlan.getArity()) {
      throw new IllegalArgumentException(
          "Exit and recursive plans should have the same arity." + "Here: " + exitPlan.getArity() + " vs " + recursivePlan.getArity());
    }

    this.exitPlan = exitPlan;
    // TODO: use a building like in MaterializationNode, in order to avoid performance bugs
    this.recursivePlan = recursivePlan.replace(delta, deltaNode).replace(full, fullNode);
  }

  public PlanNode getExitPlan() {
    return exitPlan;
  }

  public PlanNode getRecursivePlan() {
    return recursivePlan;
  }

  public PlanNode getDelta() {
    return deltaNode;
  }

  public PlanNode getFull() {
    return fullNode;
  }

  public void addRecursivePlan(PlanNode addedRecursivePlan) {
    if (addedRecursivePlan.getArity() != exitPlan.getArity()) {
      throw new IllegalArgumentException("The recursions should have the same arity as the recursion entry");
    }
    recursivePlan = recursivePlan.union(addedRecursivePlan);
  }

  @Override
  public int getArity() {
    return exitPlan.getArity();
  }

  @Override
  public boolean equals(Object obj) {
    return equals(obj, Collections.emptyMap());
  }

  @Override
  public boolean equals(Object obj, Map<PlanNode,PlanNode> assumedEqualities) {
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    RecursionNode other = (RecursionNode) obj;
    assumedEqualities = Tools.with(Tools.with(assumedEqualities, deltaNode, other.deltaNode), fullNode, other.fullNode);
    return exitPlan.equals(other.exitPlan, assumedEqualities) && recursivePlan.equals(other.recursivePlan, assumedEqualities);
  }

  @Override
  public String toString() {
    return "rec(" + exitPlan.toString() + ", " + recursivePlan.toString() + ")";
  }

  @Override
  public String operatorString() {
    return "rec_" + hash();
  }

  @Override
  public List<PlanNode> children() {
    return Arrays.asList(exitPlan, recursivePlan);
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    PlanNode newExit = exitPlan.transform(fn, this);
    PlanNode newRecursion = recursivePlan.transform(fn, this);
    PlanNode newNode = newExit.equals(exitPlan) && newRecursion.equals(recursivePlan) ? this : transform(newExit, newRecursion);
    return fn.apply(this, newNode, originalParent);
  }

  public RecursionNode transform(PlanNode exit, PlanNode recursion) {
    return new RecursionNode(exit, recursion, deltaNode, fullNode);
  }

}
