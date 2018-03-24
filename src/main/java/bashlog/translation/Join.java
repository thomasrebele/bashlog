package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.CompilerInternals;
import bashlog.command.Bash;
import bashlog.plan.SortAntiJoinNode;
import bashlog.plan.SortJoinNode;
import common.plan.node.PlanNode;

/** Translate a join to a join command. It supports a projection after the sort. Also treats antijoin. */
public class Join implements Translator {

  @Override
  public Bash translate(PlanNode planNode, CompilerInternals bc) {
    SortJoinNode j = (SortJoinNode) planNode;

    Bash.Command result = new Bash.Command("join");
    if (j instanceof SortAntiJoinNode) {
      result.arg(" -v 1 ");
    }
    result.arg("-t $'\\t'");

    if (j.getLeftProjection().length > 0) {
      int colLeft = j.getLeftProjection()[0] + 1;
      result.arg("-1 " + colLeft);
    }
    if (j.getRightProjection().length > 0) {
      int colRight = j.getRightProjection()[0] + 1;
      result.arg("-2 " + colRight);
    }

    StringBuilder outCols = new StringBuilder();
    for (int i = 0; i < j.getOutputProjection().length; i++) {
      if (i > 0) {
        outCols.append(",");
      }
      int dst = j.getOutputProjection()[i];
      if (dst < j.getLeft().getArity()) {
        outCols.append("1." + (dst + 1));
      } else {
        outCols.append("2." + (dst - j.getLeft().getArity() + 1));
      }
    }
    result.arg("-o " + outCols);

    result.file(bc.compile(j.getLeft()));
    result.file(bc.compile(j.getRight()));
    return result;
  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(SortJoinNode.class, SortAntiJoinNode.class);
  }

}
