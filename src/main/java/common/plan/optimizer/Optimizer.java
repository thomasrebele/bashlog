package common.plan.optimizer;

import java.util.function.Function;

import common.plan.node.PlanNode;

/** A plan node optimizer transforms one plan node to another one */
public interface Optimizer extends Function<PlanNode, PlanNode> {
}
