package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.BashlogCompiler;
import bashlog.command.Bash;
import common.plan.node.PlanNode;
import common.plan.node.RecursionNode;

/*public class Recursion implements Translator {

  @Override
  public Bash translate(PlanNode planNode, BashlogCompiler bc) {
      RecursionNode rn = (RecursionNode) planNode;
      int idx = tmpFileIndex++;
      String deltaFile = "tmp/delta" + idx;
      String newDeltaFile = "tmp/new" + idx;
      String fullFile = "tmp/full" + idx;
      recursionNodeToIdx.put(rn, idx);

      Bash.CommandSequence result = new Bash.CommandSequence();
      Bash b = compile(rn.getExitPlan());
      Bash.Pipe pipe = b.pipe();
      Bash.Command cmd = pipe.cmd("tee");
      cmd.file(fullFile);
      result.add(pipe.wrap("", " > " + deltaFile));

      // "do while" loop in bash
      result.cmd("while \n");

      result.add(recursionSorted(rn, fullFile, deltaFile, newDeltaFile));
      result.cmd("[ -s " + deltaFile + " ]; ");
      result.cmd("do continue; done\n");
      result.cmd("rm").file(deltaFile).wrap("", "\n");
      if (profile) result.cmd("ttime cat").file(fullFile);
      else result.cmd("cat").file(fullFile);
      return result;

  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(RecursionNode);
  }

}*/
