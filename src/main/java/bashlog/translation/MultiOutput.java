package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.BashlogCompiler;
import bashlog.command.Bash;
import common.plan.node.MultiOutputNode;
import common.plan.node.PlaceholderNode;
import common.plan.node.PlanNode;

public class MultiOutput implements Translator {

  @Override
  public Bash translate(PlanNode planNode, BashlogCompiler bc) {
    Bash.CommandSequence result = new Bash.CommandSequence();
    Bash.Command touch = result.cmd("touch");
    Bash.Command cmd = result.cmd(AwkHelper.AWK);
    MultiOutputNode mo = (MultiOutputNode) planNode;

    StringBuilder arg = new StringBuilder();
    List<PlanNode> plans = mo.reusedPlans(), nodes = mo.reuseNodes();
    for (int i = 0; i < plans.size(); i++) {
      PlanNode plan = plans.get(i), node = nodes.get(i);

      String matFile = "tmp/mat" + bc.getNextIndex();
      touch.file(matFile);
      bc.registerPlaceholder((PlaceholderNode) node, matFile);

      //TODO: if there are more conditions on one output file:
      // if (!complexAwkLine(Arrays.asList(plan), matFile, arg).isEmpty()) { ... }
      AwkHelper.simpleAwkLine(plan, matFile, arg);
    }
    cmd.arg(arg.toString()).arg("'");
    cmd.file(bc.compile(mo.getLeaf()));
    result.add(bc.compile(mo.getMainPlan()));
    return result;

  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(MultiOutputNode.class);
  }

}
