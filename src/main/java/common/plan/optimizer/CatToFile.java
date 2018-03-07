package common.plan.optimizer;

import bashlog.plan.TSVFileNode;
import common.parser.ParserReader;
import common.plan.node.BashNode;
import common.plan.node.PlanNode;

public class CatToFile implements Optimizer {

  @Override
  public PlanNode apply(PlanNode t) {
    return t.transform(pn -> {
      if (t instanceof BashNode) {
        String cmd = ((BashNode) t).getCommand();
        // TODO: support multiple file names
        if (cmd.trim().startsWith("cat ")) {
          ParserReader pr = new ParserReader(cmd);
          pr.expect("cat ");
          pr.skipWhitespace();
          String file;
          if (pr.peek() == '\"' || pr.peek() == '\'') file = pr.readString();
          else file = pr.readWhile((c, s) -> !Character.isWhitespace(c));
          pr.skipWhitespace();
          if (pr.peek() == '\0') {
            if (!file.startsWith("!")) {
              return new TSVFileNode(file, t.getArity());
            }
          }
        }
      }

      return pn;
    });
  }

}
