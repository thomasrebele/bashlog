package bashlog;

import java.util.HashMap;
import java.util.Map;

import common.parser.CompoundTerm;
import common.parser.Constant;
import common.plan.*;
import common.plan.RecursionNode.DeltaNode;

public class BashlogCompiler {

  Map<DeltaNode, String> deltaNodeToFilename = new HashMap<>();

  public String compile(PlanNode planNode, String indent) {
    StringBuilder sb = new StringBuilder();
    compile(planNode, sb, indent);
    return sb.toString();
  }

  public String compile(PlanNode planNode) {
    return compile(planNode, "");
  }

  private static String keyMask(int[] columns) {
    StringBuilder mask = new StringBuilder();
    for (int i = 0; i < columns.length; i++) {
      if (mask.length() > 0) mask.append(" FS ");
      mask.append("$");
      mask.append(columns[i] + 1);
    }
    return mask.toString();

  }

  private static String escape(String str) {
    return str.replaceAll("\"", "\\\"").replaceAll("'", "'\\''");
  }

  final static String INDENT = "    ";

  private void leftHashJoin(JoinNode j, StringBuilder sb, String indent) {
    sb.append(indent + "awk -v FS=$'\\t' 'NR==FNR { ");
    sb.append("key = ");
    sb.append(keyMask(j.getLeftJoinProjection()));
    sb.append("; h[key] = $0; ");
    sb.append("next } \n");
    sb.append(indent);
    sb.append(" { ");
    sb.append("key = ");
    sb.append(keyMask(j.getRightJoinProjection()));
    sb.append("; if (key in h) {");
    sb.append(" print h[key] FS $0");
    sb.append(" } }' \\\n");
    sb.append(indent);
    sb.append(" <(");
    compile(j.getLeft(), sb, indent + INDENT);
    sb.append(")");
    sb.append(" \\\n");
    sb.append(indent);
    sb.append(" <(");
    compile(j.getRight(), sb, indent + INDENT);
    sb.append(")");
  }

  private void compile(PlanNode planNode, StringBuilder sb, String indent) {
    if (planNode instanceof JoinNode) {
      JoinNode j = (JoinNode) planNode;
      leftHashJoin(j, sb, indent);
    } else if (planNode instanceof ProjectNode) {
      ProjectNode p = ((ProjectNode) planNode);
      compile(p.getTable(), sb, indent + INDENT);
      sb.append(" \\\n");
      sb.append(indent);
      sb.append("| awk -v FS=$'\\t' '{ print ");
      for (int i = 0; i < p.getProjection().length; i++) {
        if (i != 0) sb.append(" FS ");
        if (p.getProjection()[i] >= 0) {
          sb.append("$");
          sb.append(p.getProjection()[i] + 1);
        } else if (p.getConstants().length >= i) {
          Object cnst = p.getConstants()[i];
          if (cnst != null) {
            sb.append("\"" + escape(cnst.toString()) + "\"");
          }
        }
      }
      sb.append("}'");
    } else if (planNode instanceof ConstantEqualityFilterNode) {
      ConstantEqualityFilterNode n = (ConstantEqualityFilterNode) planNode;
      sb.append("awk -v FS=$'\\t' ' $");
      sb.append(n.getField() + 1);
      sb.append(" == \"");
      sb.append(escape(n.getValue().toString()));
      sb.append("\" { print $0 }' \\\n");
      sb.append(indent);
      sb.append(" <(\\\n");
      compile(n.getTable(), sb, indent + INDENT);
      sb.append(")");
    } else if (planNode instanceof BuiltinNode) {
      CompoundTerm ct = ((BuiltinNode) planNode).compoundTerm;
      if ("bash_command".equals(ct.name)) {
        sb.append(((Constant) ct.args[0]).getValue());
      }
    } else if (planNode instanceof UnionNode) {
      sb.append("cat \\\n");
      for (PlanNode child : ((UnionNode) planNode).getChildren()) {
        sb.append(indent);
        sb.append("<( \\\n");
        compile(child, sb, indent + INDENT);
        sb.append(") \\\n");
      }
    } else if (planNode instanceof RecursionNode) {
      RecursionNode rn = (RecursionNode) planNode;
      String deltaFile = "tmp_delta" + deltaNodeToFilename.size();
      String newDeltaFile = "tmp_new" + deltaNodeToFilename.size();
      String fullFile = "tmp_full" + deltaNodeToFilename.size();
      deltaNodeToFilename.put(rn.getDelta(), deltaFile);
      sb.append(indent + "echo -n '' > " + deltaFile + "\n" + indent);
      sb.append(indent + "echo -n '' > " + fullFile + "\n" + indent);
      compile(rn.getExitPlan(), sb, indent + INDENT);
      sb.append(" | tee " + fullFile + " > " + deltaFile + "\n");
      sb.append(indent + "while \n");
      compile(rn.getRecursivePlan(), sb, indent + INDENT);
      sb.append(" \\\n" + indent);
      sb.append(INDENT + "| grep -v -F -f " + fullFile);
      sb.append(" | tee -a " + fullFile);
      sb.append(" > " + newDeltaFile + "\n");

      sb.append(indent + INDENT + "mv " + newDeltaFile + " " + deltaFile + "; \n");
      sb.append(indent + INDENT + "[ -s " + deltaFile + " ]; \n");
      sb.append(indent + "do continue; done\n");
      sb.append(indent + "rm " + deltaFile + "\n");
      sb.append("cat " + fullFile);
    } else if (planNode instanceof DeltaNode) {
      sb.append("cat " + deltaNodeToFilename.get(planNode));

    } else {
      System.err.println("compilation of " + planNode.getClass() + " not yet supported");
    }
  }

}
