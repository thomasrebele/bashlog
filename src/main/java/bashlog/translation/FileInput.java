package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.CompilerInternals;
import bashlog.command.Bash;
import bashlog.plan.TSVFileNode;
import common.plan.node.PlanNode;

/** Translate a file to a file */
public class FileInput implements Translator {

  @Override
  public Bash translate(PlanNode planNode, CompilerInternals bc) {
    TSVFileNode file = (TSVFileNode) planNode;
    return new Bash.BashFile(file.getPath());
  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(TSVFileNode.class);
  }

}
