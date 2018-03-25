package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.CompilerInternals;
import bashlog.command.Bash;
import common.plan.node.ConstantEqualityFilterNode;
import common.plan.node.PlanNode;
import common.plan.node.ProjectNode;
import common.plan.node.VariableEqualityFilterNode;

/** Translates projects and filters to an AWK command */
public class ProjectFilter implements BashTranslator {

  @Override
  public Bash translate(PlanNode planNode, CompilerInternals bc) {
      StringBuilder awk = new StringBuilder();
      PlanNode inner = AwkHelper.simpleAwkLine(planNode, null, awk);

      StringBuilder advAwk = new StringBuilder();
      if (AwkHelper.complexAwkLine(Arrays.asList(planNode), null, advAwk).isEmpty()) {
        awk = advAwk;
      }

      return new Bash.Command(AwkHelper.AWK).arg(awk.toString()).arg("'").file(bc.compile(inner));
  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(ProjectNode.class, ConstantEqualityFilterNode.class, VariableEqualityFilterNode.class);
  }

}
