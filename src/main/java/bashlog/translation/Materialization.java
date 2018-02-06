package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.BashlogCompiler;
import bashlog.command.Bash;
import common.plan.node.MaterializationNode;
import common.plan.node.PlanNode;

public class Materialization implements Translator {

  @Override
  public Bash translate(PlanNode planNode, BashlogCompiler bc) {
    MaterializationNode m = (MaterializationNode) planNode;
    String matFile = "tmp/mat" + bc.getNextIndex();
    bc.registerPlaceholder(m.getReuseNode(), matFile);
    Bash.CommandSequence result = new Bash.CommandSequence();
    result.comment(planNode, "");
    result.info(planNode, "");

    Bash reused = bc.compile(m.getReusedPlan());
    if (bc.parallelMaterialization()) {
      String lockFile = matFile.replaceAll("tmp/", "tmp/lock_");
      String doneFile = matFile.replaceAll("tmp/", "tmp/done_");
      reused = reused.wrap("mkfifo " + lockFile + "; ( ", //
          " > " + matFile + //
              "; mv " + lockFile + " " + doneFile + //
              "; cat " + doneFile + " > /dev/null & " + //
              "exec 3> " + doneFile + "; exec 3>&-;" + //
              " ) & ");
    } else {
      reused = reused.wrap("", " > " + matFile);
    }
    result.add(reused);

    if (!(m.getMainPlan() instanceof MaterializationNode)) {
      result.other("\n# plan");
    }
    result.add(bc.compile(m.getMainPlan()));
    return result;

  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(MaterializationNode.class);
  }

}
