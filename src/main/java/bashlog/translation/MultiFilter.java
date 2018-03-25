package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.CompilerInternals;
import bashlog.command.Bash;
import common.plan.node.MultiFilterNode;
import common.plan.node.PlanNode;

/**
 * Translate multi filter plan to one AWK command.
 * A multi filter are (a union of) filters, projections on the same plan, that build one single plan.
 */
public class MultiFilter implements BashTranslator {

  @Override
  public Bash translate(PlanNode planNode, CompilerInternals bc) {
      MultiFilterNode m = (MultiFilterNode) planNode;
      Bash.Command cmd = new Bash.Command(AwkHelper.AWK);

      StringBuilder arg = new StringBuilder();
      List<PlanNode> remaining = AwkHelper.complexAwkLine(m.getFilter(), null, arg);

      // process remaining filter nodes
      for (PlanNode c : remaining) {
        AwkHelper.simpleAwkLine(c, null, arg);
      }
      arg.append("' ");
      cmd.arg(arg.toString());
      cmd.file(bc.compile(m.getTable()));

      return cmd;
  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(MultiFilterNode.class);
  }

}
