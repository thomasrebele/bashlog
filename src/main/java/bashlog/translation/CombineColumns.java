package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.CompilerInternals;
import bashlog.command.Bash;
import bashlog.plan.CombinedColumnNode;
import common.plan.node.PlanNode;

public class CombineColumns implements Translator {

  @Override
  public Bash translate(PlanNode planNode, CompilerInternals bc) {
    CombinedColumnNode c = (CombinedColumnNode) planNode;
    Bash prev = bc.compile(c.getTable());

    Bash.Pipe result = new Bash.Pipe(prev);
    Bash.Command cmd = result.cmd(AwkHelper.AWK);
    StringBuilder sb = new StringBuilder();
    sb.append("{ print $0 FS ");
    for (int i = 0; i < c.getColumns().length; i++) {
      if (i > 0) {
        sb.append(" \"\\002\" ");
      }
      sb.append("$" + (c.getColumns()[i] + 1));
    }
    sb.append("}'");
    cmd.arg(sb.toString());
    return result;

  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(CombinedColumnNode.class);
  }

}
