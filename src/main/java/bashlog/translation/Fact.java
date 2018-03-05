package bashlog.translation;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import bashlog.CompilerInternals;
import bashlog.command.Bash;
import common.plan.node.FactNode;
import common.plan.node.PlanNode;

/** Translate a join to a join command. It supports a projection after the sort. Also treats antijoin. */
public class Fact implements Translator {

  @Override
  public Bash translate(PlanNode planNode, CompilerInternals bc) {
    FactNode j = (FactNode) planNode;

    Bash.CommandSequence result = new Bash.CommandSequence();
    for (Comparable<?>[] fact : j.getFacts()) {
      String content = Arrays.stream(fact).map(f -> f.toString()).collect(Collectors.joining("\t"));

      Bash.Command c = new Bash.Command("cat ");
      c.heredoc(new Bash.Heredoc("EOF", content));
      result.add(c);
    }
    return result;
  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(FactNode.class);
  }

}
