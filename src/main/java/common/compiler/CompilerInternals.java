package common.compiler;

import java.util.Map;

import common.plan.node.PlaceholderNode;
import common.plan.node.PlanNode;

public class CompilerInternals<_Translator> {

  protected final Map<Class<?>, _Translator> translators;

  protected Map<PlaceholderNode, PlanNode> placeholderToParent;

  public CompilerInternals(Map<Class<?>, _Translator> translators, PlanNode fullPlan) {
    this.translators = translators;
    placeholderToParent = PlaceholderNode.placeholderToParentMap(fullPlan);
  }

  public PlanNode getParent(PlaceholderNode pn) {
    return (PlanNode) placeholderToParent.get(pn);
  }

}
