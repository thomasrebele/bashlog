package bashlog;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import bashlog.plan.SortJoinNode;
import bashlog.plan.SortNode;
import bashlog.plan.SortRecursionNode;
import bashlog.plan.SortUnionNode;
import common.parser.CompoundTerm;
import common.parser.Constant;
import common.plan.*;
import common.plan.MaterializationNode.ReuseNode;
import common.plan.RecursionNode.DeltaNode;

public class BashlogCompiler {

  int tmpFileIndex = 0;

  Map<RecursionNode, String> recursionNodeToFilename = new HashMap<>();

  Map<MaterializationNode, String> matNodeToFilename = new HashMap<>();

  PlanNode root;

  public BashlogCompiler(PlanNode planNode) {
    root = new PlanSimplifier().apply(planNode);
    root = new PushDownFilterOptimizer().apply(root);

    System.out.println("optimized");
    System.out.println(root.toPrettyString());

    root = root.transform(this::transform);
    root = new BashlogOptimizer().apply(root);
    root = new MaterializationOptimizer().apply(root);

    System.out.println("bashlog plan");
    System.out.println(root.toPrettyString());

  }

  public String compile() {
    return compile("");
  }

  public String compile(String indent) {
    StringBuilder sb = new StringBuilder();
    sb.append("#!/bin/bash\n");
    sb.append("export LC_ALL=C\n");
    sb.append("mkdir -p tmp\n");
    compile(root, sb, indent);
    return sb.toString();
  }


  private PlanNode transform(PlanNode p) {
    if (p instanceof JoinNode) {
      // sort join
      JoinNode joinNode = (JoinNode) p;
      PlanNode left = new SortNode(joinNode.getLeft(), joinNode.getLeftJoinProjection());
      PlanNode right = new SortNode(joinNode.getRight(), joinNode.getRightJoinProjection());
      return new SortJoinNode(left, right, joinNode.getLeftJoinProjection(), joinNode.getRightJoinProjection());
    } else if (p instanceof RecursionNode) {
      RecursionNode r = (RecursionNode) p;
      return new SortRecursionNode(new SortNode(r.getExitPlan(), null), new SortNode(r.getRecursivePlan(), null), r.getDelta(), r.getFull());
    } else if (p instanceof UnionNode) {
      UnionNode u = (UnionNode) p;
      if (u.args().size() > 2) {
        throw new UnsupportedOperationException("Only unions with two arguments supported");
      }
      return new SortUnionNode(u.args().stream().map(c -> new SortNode(c, null)).collect(Collectors.toSet()), u.getArity());
    }

    return p;
  }



  /** Awk colums used as key for join */
  private static String keyMask(int[] columns) {
    StringBuilder mask = new StringBuilder();
    for (int i = 0; i < columns.length; i++) {
      if (mask.length() > 0) mask.append(" FS ");
      mask.append("$");
      mask.append(columns[i] + 1);
    }
    return mask.toString();

  }

  /** Escape string for usage in awk */
  private static String escape(String str) {
    return str.replaceAll("\"", "\\\"").replaceAll("'", "'\\''");
  }

  final static String INDENT = "    ";

  final static String AWK = "awk -v FS=$'\\t' '";

  /** Awk command which caches the left subtree, and joins it with the right subtree */
  private void leftHashJoin(JoinNode j, StringBuilder sb, String indent) {
    sb.append(indent + AWK + "NR==FNR { ");
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

  /**
   * Sort the output of n on columns; might need to introduce a new column
   * @param n
   * @param cols
   * @param sb
   * @param indent
   * @return column which is sorted (1-based index)
   */
  private void sort(PlanNode n, int[] cols, StringBuilder sb, String indent) {
    compile(n, sb, indent + INDENT);
    sb.append(" \\\n" + indent);
    if (cols != null && cols.length > 1) {
      sb.append(" | " + AWK + "{ print $0 FS ");
      for (int i = 0; i < cols.length; i++) {
        if (i > 0) {
          sb.append(",");
        }
        sb.append("$");
        sb.append(cols[i] + 1);
      }
      sb.append("}'");
    }
    sb.append(" | sort -t $'\\t' ");
    if (cols != null) {
      sb.append("-k ");
      sb.append(sortCol(n, cols));
    }
  }

  private int sortCol(PlanNode n, int[] cols) {
    if (cols.length > 1) {
      return n.getArity() + 1;
    }
    return cols[0] + 1;
  }

  /** Sort left and right tree, and join with 'join' command */
  private void sortJoin(SortJoinNode j, StringBuilder sb, String indent) {
    int colLeft, colRight;
    colLeft = sortCol(j.getLeft(), j.getLeftJoinProjection());
    colRight = sortCol(j.getRight(), j.getRightJoinProjection());

    sb.append("join -t $'\\t' -1 ");
    sb.append(colLeft);
    sb.append(" -2 ");
    sb.append(colRight);
    sb.append(" -o ");
    for (int i = 0; i < j.getLeft().getArity(); i++) {
      if (i > 0) sb.append(",");
      sb.append("1." + (i + 1));
    }
    sb.append(",");
    for (int i = 0; i < j.getRight().getArity(); i++) {
      if (i > 0) sb.append(",");
      sb.append("2." + (i + 1));
    }

    sb.append(" <( \\\n");
    compile(j.getLeft(), sb, indent);
    sb.append(") <( \\\n");
    compile(j.getRight(), sb, indent);
    sb.append(")");
  }

  /** Remove all lines from pipe that occur in filename */
  private void setMinusInMemory(String filename, StringBuilder sb) {
    sb.append("| grep -v -F -f ");
    sb.append(filename);
  }

  private void setMinusSorted(String filename, StringBuilder sb) {
    sb.append("| comm --nocheck-order -23 - ");
    sb.append(filename);
  }

  private void recursionSorted(RecursionNode rn, StringBuilder sb, String indent, String fullFile, String deltaFile, String newDeltaFile) {
    compile(rn.getRecursivePlan(), sb, indent + INDENT);
    sb.append(" \\\n" + indent + INDENT);
    //setMinusInMemory(fullFile, sb);
    setMinusSorted(fullFile, sb);
    sb.append(" > " + newDeltaFile + "\n");

    sb.append(indent + INDENT + "mv " + newDeltaFile + " " + deltaFile + "; \n");
    sb.append(indent + INDENT + "sort -u --merge -o " + fullFile + " " + fullFile + " <(sort " + deltaFile + ")\n");
  }

  private void recursionInMemory(RecursionNode rn, StringBuilder sb, String indent, String fullFile, String deltaFile, String newDeltaFile) {
    compile(rn.getRecursivePlan(), sb, indent + INDENT);
    sb.append(" \\\n" + indent);
    sb.append(INDENT);
    setMinusInMemory(fullFile, sb);
    sb.append(" | tee -a " + fullFile);
    sb.append(" > " + newDeltaFile + "\n");
    sb.append(indent + INDENT + "mv " + newDeltaFile + " " + deltaFile + "; \n");
  }

  private void compile(PlanNode planNode, StringBuilder sb, String indent) {
    if (planNode instanceof MaterializationNode) {
      MaterializationNode m = (MaterializationNode) planNode;
      String matFile = "tmp/mat" + tmpFileIndex++;
      matNodeToFilename.putIfAbsent(m, matFile);

      boolean asFile = true;
      sb.append("\n").append(indent).append("# ").append(m.operatorString()).append("\n");
      /*if (!asFile) {
        sb.append("mkfifo");
        for (int i = 0; i < info.planUseCount; i++) {
          sb.append(" " + info.filename + "_" + i);
        }
        sb.append("\n");
      }*/
      compile(m.getReusedPlan(), sb, indent);
      sb.append(" \\\n");
      if (asFile) {
        sb.append(" > ");
        sb.append(matFile);
      } else {
        /*for (int i = 0; i < info.planUseCount; i++) {
          sb.append(i < info.planUseCount - 1 ? " | tee " : " > ");
          sb.append(info.filename + "_" + i);
        }
        sb.append(" &");*/
        throw new UnsupportedOperationException("pipes not yet supported");
      }
      sb.append("\n");
      
      if (!(m.getMainPlan() instanceof MaterializationNode)) {
        sb.append("# plan\n");
      }
      compile(m.getMainPlan(), sb, indent);

    } else if (planNode instanceof ReuseNode) {
      sb.append("cat " + matNodeToFilename.get(((ReuseNode) planNode).getMaterializeNode()));
      // TODO: pipes
      /*if (!info.materialize()) {
        sb.append("_" + info.bashUseCount++);
      }*/

    } else if (planNode instanceof SortNode) {
      sort(planNode.args().get(0), ((SortNode) planNode).sortColumns(), sb, indent);
    } else if (planNode instanceof SortJoinNode) {
      SortJoinNode j = (SortJoinNode) planNode;
      //leftHashJoin(j, sb, indent);
      sortJoin(j, sb, indent);
    } else if (planNode instanceof ProjectNode) {
      // TODO: filtering of duplicates might be necessary
      ProjectNode p = ((ProjectNode) planNode);
      compile(p.getTable(), sb, indent + INDENT);
      sb.append(" \\\n");
      sb.append(indent);
      sb.append("| " + AWK + "{ print ");
      for (int i = 0; i < p.getProjection().length; i++) {
        if (i != 0) sb.append(" FS ");
        if (p.getProjection()[i] >= 0) {
          sb.append("$");
          sb.append(p.getProjection()[i] + 1);
        } else {
          p.getConstant(i).ifPresent(cnst -> sb.append("\"" + escape(cnst.toString()) + "\""));
        }
      }
      sb.append("}'");
    } else if (planNode instanceof ConstantEqualityFilterNode) {
      ConstantEqualityFilterNode n = (ConstantEqualityFilterNode) planNode;
      sb.append(AWK + "$");
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
        sb.append(indent + ((Constant) ct.args[0]).getValue());
      }
    } else if (planNode instanceof SortUnionNode) {
      sb.append("comm --output-delimiter='' -3 \\\n");
      for (PlanNode child : ((UnionNode) planNode).getChildren()) {
        sb.append(indent);
        sb.append("<( \\\n");
        compile(child, sb, indent + INDENT);
        sb.append(") \\\n");
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
      int idx = tmpFileIndex++;
      String deltaFile = "tmp/delta" + idx;
      String newDeltaFile = "tmp/new" + idx;
      String fullFile = "tmp/full" + idx;
      recursionNodeToFilename.put(rn, deltaFile);
      compile(rn.getExitPlan(), sb, indent + INDENT);
      sb.append(" | tee " + fullFile + " > " + deltaFile + "\n");
      // "do while" loop in bash
      sb.append(indent + "while \n");

      recursionSorted(rn, sb, indent, fullFile, deltaFile, newDeltaFile);
      //recursionInMemory(rn, sb, indent, fullFile, deltaFile, newDeltaFile);

      sb.append(indent + INDENT + "[ -s " + deltaFile + " ]; \n");
      sb.append(indent + "do continue; done\n");
      //sb.append(indent + "rm " + deltaFile + "\n");
      sb.append("cat " + fullFile);
    } else if (planNode instanceof DeltaNode) {
      sb.append("cat " + recursionNodeToFilename.get(((DeltaNode) planNode).getRecursionNode()));
    } else {
      System.err.println("compilation of " + planNode.getClass() + " not yet supported");
    }
  }

}
