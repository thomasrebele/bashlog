package common.plan.optimizer;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.plan.node.PlanNode;

/** A plan node optimizer transforms one plan node to another one */
public interface Optimizer extends Function<PlanNode, PlanNode> {

  static PlanNode applyOptimizer(PlanNode root, List<List<Optimizer>> stages) {
    for (List<Optimizer> stage : stages) {
      for (Optimizer o : stage) {
        root = o.apply(root);
      }
    }
    return root;
  }

  static PlanNode applyOptimizer(PlanNode root, List<String> stageNames, List<List<Optimizer>> stages, StringBuilder debugBuilder) {
    Iterator<String> it = stageNames.iterator();
    PlanValidator check = new PlanValidator(debugBuilder);
    for (List<Optimizer> stage : stages) {
      debugBuilder.append("\n\n").append(it.hasNext() ? it.next() : "").append("\n");
      for (Optimizer o : stage) {
        PlanNode prevPlan = root;
        try {
          root = o.apply(root);

          debugBuilder.append("applied ").append(o.getClass()).append(" \n");
          debugBuilder.append(root.toPrettyString()).append("\n");

          check.apply(root);
        } catch (Exception e) {
          System.err.println("some problems while applying " + o + " to plan");
          System.err.println(prevPlan.toPrettyString());

          LogHolder.LOG.error(e.getMessage());
          debugBuilder.append("WARNING: ").append(e.getMessage());
          //throw e;
        }
      }
    }
    return root;
  }
}

final class LogHolder {

  static final Logger LOG = LoggerFactory.getLogger(Optimizer.class);
}
