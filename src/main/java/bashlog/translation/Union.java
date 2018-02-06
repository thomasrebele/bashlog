package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.BashlogCompiler;
import bashlog.command.Bash;
import common.plan.node.PlanNode;
import common.plan.node.UnionNode;

public class Union implements Translator {

  @Override
  public Bash translate(PlanNode planNode, BashlogCompiler bc) {
    if (planNode.children().size() == 0) {
      return new Bash.Command("echo").arg("-n");
    } else {
      Bash.Command result = new Bash.Command("$sort").arg("-u");
      for (PlanNode child : ((UnionNode) planNode).getChildren()) {
        result.file(bc.compile(child));
      }
      return result;
    }
  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(UnionNode.class);
  }

}
