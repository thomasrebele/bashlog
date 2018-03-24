package common.compiler;

import java.util.List;

import common.plan.node.PlanNode;

public interface Translator<Output, Internals> {

  /**
   * Translate planNode to bash snippet
   * @param planNode
   * @param bc helps to translate children plans, and to obtain temporary file numbers
   * @return
   */
  public Output translate(PlanNode planNode, Internals bc);

  /** Which node types are supported by this translator */
  public List<Class<?>> supports();
}