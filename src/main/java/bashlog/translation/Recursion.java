package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.CompilerInternals;
import bashlog.command.Bash;
import common.plan.node.PlanNode;
import common.plan.node.RecursionNode;

/** Translates a recursion node to a bash while loop */
public class Recursion implements BashTranslator {

  private Bash setMinusSorted(Bash prev, String filename) {
    Bash.Pipe result = prev.pipe();
    result.cmd("comm")//
        .arg("-23").arg("-")//
        .file(filename);
    return result;
  }

  private Bash recursionSorted(CompilerInternals bc, RecursionNode rn, String fullFile, String deltaFile, String newDeltaFile) {
    Bash prev = bc.compile(rn.getRecursivePlan());
    //setMinusInMemory(fullFile, sb);
    Bash delta = setMinusSorted(prev, fullFile);
    delta = delta.wrap("", " > " + newDeltaFile + ";");

    Bash.CommandSequence result = new Bash.CommandSequence();
    result.add(delta);
    result.info(rn, "continued");
    result.cmd("mv").file(newDeltaFile).file(deltaFile).arg("; ");
    result.cmd("$sort")//
        .arg("-u").arg("--merge").arg("-o")//
        .file(fullFile).file(fullFile).file(deltaFile).arg("; ");

    return result;
  }

  @Override
  public Bash translate(PlanNode planNode, CompilerInternals bc) {
    RecursionNode rn = (RecursionNode) planNode;
    int idx = bc.getNextIndex();
    String deltaFile = "tmp/delta" + idx;
    String newDeltaFile = "tmp/new" + idx;
    String fullFile = "tmp/full" + idx;
    bc.registerPlaceholder(rn.getDelta(), deltaFile);
    bc.registerPlaceholder(rn.getFull(), fullFile);

    Bash.CommandSequence result = new Bash.CommandSequence();
    Bash b = bc.compile(rn.getExitPlan());
    Bash.Pipe pipe = b.pipe();
    Bash.Command cmd = pipe.cmd("tee");
    cmd.file(fullFile);
    result.add(pipe.wrap("", " > " + deltaFile));

    // "do while" loop in bash
    result.cmd("while \n");

    result.add(recursionSorted(bc, rn, fullFile, deltaFile, newDeltaFile));
    result.cmd("[ -s " + deltaFile + " ]; ");
    result.cmd("do continue; done\n");
    result.cmd("rm").file(deltaFile).wrap("", "\n");
    result.cmd("cat").file(fullFile);
    return result;

  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(RecursionNode.class);
  }

}
