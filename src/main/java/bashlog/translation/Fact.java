package bashlog.translation;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import bashlog.CompilerInternals;
import bashlog.command.Bash;
import common.plan.node.FactNode;
import common.plan.node.PlanNode;

/** Translate a fact node that represents a list of facts directly stored in the datalog program. */
public class Fact implements BashTranslator {

  @Override
  public Bash translate(PlanNode planNode, CompilerInternals bc) {
    FactNode j = (FactNode) planNode;

    Bash.CommandSequence result = new Bash.CommandSequence();
    Bash.Command c = new Bash.Command("cat ");
    StringBuilder content = new StringBuilder();
    for (Comparable<?>[] fact : j.getFacts()) {
      content.append(Arrays.stream(fact).map(f -> f.toString()).collect(Collectors.joining("\t")));
      content.append("\n");
    }
    c.heredoc(new Bash.Heredoc("EOF", content.toString()));
    
    // wrap in a function
    Bash function = c.wrap("relation() {\n", "}");
    result.add(function);
    result.add(new Bash.Command("relation"));
    return result;
  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(FactNode.class);
  }

}
