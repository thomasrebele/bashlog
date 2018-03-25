package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.CompilerInternals;
import bashlog.command.Bash;
import common.plan.node.PlanNode;
import common.plan.node.UnionNode;

/** Translates a union node to a sort command, which removes duplicates */
public class Union implements BashTranslator {

  @Override
  public Bash translate(PlanNode planNode, CompilerInternals bc) {
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
