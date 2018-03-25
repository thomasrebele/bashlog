package bashlog.translation;

import java.util.Arrays;
import java.util.List;

import bashlog.CompilerInternals;
import bashlog.command.Bash;
import common.parser.CompoundTerm;
import common.parser.Constant;
import common.parser.ParserReader;
import common.plan.node.PlanNode;
import common.plan.node.BashNode;
import common.plan.node.BuiltinNode;

/** Translate a builtin command, which is a bash command, to a bash command */
public class BashCmd implements Translator {

  @Override
  public Bash translate(PlanNode planNode, CompilerInternals ci) {
    BashNode bn = (BashNode) planNode;
    List<String> cmdParts = bn.getCommandParts();
    Bash.Command bc = new Bash.Command(cmdParts.get(0));

    List<PlanNode> children = bn.children();
    for (int i = 0; i < children.size(); i++) {
      bc.file(ci.compile(children.get(i)));
      bc.arg(cmdParts.get(i + 1));
    }

    // instead of "<(cat file)", use file directly
    String cmd = bn.getCommand();
    if (cmd.startsWith("cat ")) {
      ParserReader pr = new ParserReader(cmd);
      pr.expect("cat ");
      pr.skipWhitespace();
      String file;
      if (pr.peek() == '\"' || pr.peek() == '\'') file = pr.readString();
      else file = pr.readWhile((c, s) -> !Character.isWhitespace(c));
      pr.skipWhitespace();
      if (pr.peek() == '\0') {
        if (!file.startsWith("!")) {
          return new Bash.BashFile(file);
        }
        else {
          return ci.compile(children.get(0));
        }
      }
    }

    return bc;
  }

  @Override
  public List<Class<?>> supports() {
    return Arrays.asList(BashNode.class);
  }

}
