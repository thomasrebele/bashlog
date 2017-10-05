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

  enum Context {
    NONE, FILE, PIPE;
    
    public void start(Context next, StringBuilder sb) {
      if (this == FILE && next == PIPE) {
        sb.append(" <(");
      }
      if (this == FILE && next == FILE) {
        sb.append(" ");
      }
      if (this == PIPE && next == FILE) {
        sb.append(" [START FILE CONTEXT] ");
      }
    }

    public void end(Context next, StringBuilder sb) {
      if (this == FILE && next == PIPE) {
        sb.append(")");
      }
      if (this == PIPE && next == FILE) {
        sb.append(" [END FILE CONTEXT] ");
      }
    }

    public void expect(Context next) {
      if (this != next) {
        throw new UnsupportedOperationException("cannot switch to context " + next);
      }
    }
  }

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
    compile(root, sb, indent, Context.PIPE);
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
  private void leftHashJoin(JoinNode j, StringBuilder sb, String indent, Context ctx) {
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
    compile(j.getLeft(), sb, indent + INDENT, Context.FILE);
    sb.append(" \\\n");
    sb.append(indent);
    compile(j.getRight(), sb, indent + INDENT, Context.FILE);
  }

  /**
   * Sort the output of n on columns; might need to introduce a new column
   * @param n
   * @param cols
   * @param sb
   * @param indent
   * @return column which is sorted (1-based index)
   */
  private void sort(PlanNode n, int[] cols, StringBuilder sb, String indent, Context ctx) {
    ctx.start(Context.PIPE, sb);
    compile(n, sb, indent + INDENT, Context.PIPE);
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
    ctx.end(Context.PIPE, sb);
  }

  private int sortCol(PlanNode n, int[] cols) {
    if (cols.length > 1) {
      return n.getArity() + 1;
    }
    return cols[0] + 1;
  }

  /** Sort left and right tree, and join with 'join' command */
  private void sortJoin(SortJoinNode j, StringBuilder sb, String indent, Context ctx) {
    ctx.start(Context.PIPE, sb);
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
    sb.append(" ");
    compile(j.getLeft(), sb, indent, Context.FILE);
    sb.append(" \\\n");
    compile(j.getRight(), sb, indent, Context.FILE);
    sb.append("");
    ctx.end(Context.PIPE, sb);
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

  private void recursionSorted(RecursionNode rn, StringBuilder sb, String indent, Context ctx, String fullFile, String deltaFile,
      String newDeltaFile) {
    ctx.start(Context.PIPE, sb);
    compile(rn.getRecursivePlan(), sb, indent + INDENT, Context.PIPE);
    sb.append(" \\\n" + indent + INDENT);
    //setMinusInMemory(fullFile, sb);
    setMinusSorted(fullFile, sb);
    sb.append(" > " + newDeltaFile + "\n");

    sb.append(indent + INDENT + "mv " + newDeltaFile + " " + deltaFile + "; \n");
    sb.append(indent + INDENT + "sort -u --merge -o " + fullFile + " " + fullFile + " <(sort " + deltaFile + ")\n");
    ctx.end(Context.PIPE, sb);
  }

  private void recursionInMemory(RecursionNode rn, StringBuilder sb, String indent, Context ctx, String fullFile, String deltaFile,
      String newDeltaFile) {
    ctx.start(Context.PIPE, sb);
    compile(rn.getRecursivePlan(), sb, indent + INDENT, Context.PIPE);
    sb.append(" \\\n" + indent);
    sb.append(INDENT);
    setMinusInMemory(fullFile, sb);
    sb.append(" | tee -a " + fullFile);
    sb.append(" > " + newDeltaFile + "\n");
    sb.append(indent + INDENT + "mv " + newDeltaFile + " " + deltaFile + "; \n");
    ctx.end(Context.PIPE, sb);
  }

  private void compile(PlanNode planNode, StringBuilder sb, String indent, Context ctx) {
    if (planNode instanceof MaterializationNode) {
      ctx.start(Context.PIPE, sb);
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
      compile(m.getReusedPlan(), sb, indent, Context.PIPE);
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
      compile(m.getMainPlan(), sb, indent, Context.PIPE);
      ctx.end(Context.PIPE, sb);

    } else if (planNode instanceof ReuseNode) {
      String matFile = matNodeToFilename.get(((ReuseNode) planNode).getMaterializeNode());
      if (ctx == Context.FILE) {
        sb.append(matFile);
      }
      else {
        ctx.start(Context.PIPE, sb);
        sb.append("cat " + matFile);
        ctx.end(Context.PIPE, sb);
      }
      // TODO: pipes
      /*if (!info.materialize()) {
        sb.append("_" + info.bashUseCount++);
      }*/

    } else if (planNode instanceof SortNode) {
      sort(planNode.args().get(0), ((SortNode) planNode).sortColumns(), sb, indent, ctx);
    } else if (planNode instanceof SortJoinNode) {
      SortJoinNode j = (SortJoinNode) planNode;
      //leftHashJoin(j, sb, indent);
      sortJoin(j, sb, indent, ctx);
    } else if (planNode instanceof ProjectNode) {
      // TODO: filtering of duplicates might be necessary
      ctx.start(Context.PIPE, sb);
      ProjectNode p = ((ProjectNode) planNode);
      compile(p.getTable(), sb, indent + INDENT, Context.PIPE);
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
      ctx.end(Context.PIPE, sb);
    } else if (planNode instanceof ConstantEqualityFilterNode) {
      ctx.start(Context.PIPE, sb);
      ConstantEqualityFilterNode n = (ConstantEqualityFilterNode) planNode;
      sb.append(AWK + "$");
      sb.append(n.getField() + 1);
      sb.append(" == \"");
      sb.append(escape(n.getValue().toString()));
      sb.append("\" { print $0 }' \\\n");
      sb.append(indent);
      compile(n.getTable(), sb, indent + INDENT, Context.FILE);
      ctx.end(Context.PIPE, sb);
    } else if (planNode instanceof BuiltinNode) {
      CompoundTerm ct = ((BuiltinNode) planNode).compoundTerm;
      if ("bash_command".equals(ct.name)) {
        ctx.start(Context.PIPE, sb);
        sb.append(indent + ((Constant) ct.args[0]).getValue());
        ctx.end(Context.PIPE, sb);
      }
      else {
        throw new UnsupportedOperationException("predicate not supported: " + ct.getRelation());
      }
    } else if (planNode instanceof SortUnionNode) {
      ctx.start(Context.PIPE, sb);
      // delimit columns by null character
      sb.append("comm --output-delimiter=$'\\0' \\\n");
      for (PlanNode child : ((UnionNode) planNode).getChildren()) {
        sb.append(indent);
        compile(child, sb, indent + INDENT, Context.FILE);
      }
      // remove null character
      sb.append("| sed -E 's/^\\x0\\x0?//g'"); // 
      ctx.end(Context.PIPE, sb);
    } else if (planNode instanceof UnionNode) {
      ctx.start(Context.PIPE, sb);
      sb.append("cat \\\n");
      for (PlanNode child : ((UnionNode) planNode).getChildren()) {
        compile(child, sb, indent + INDENT, Context.FILE);
      }
      ctx.end(Context.PIPE, sb);
    } else if (planNode instanceof RecursionNode) {
      ctx.start(Context.PIPE, sb);
      RecursionNode rn = (RecursionNode) planNode;
      int idx = tmpFileIndex++;
      String deltaFile = "tmp/delta" + idx;
      String newDeltaFile = "tmp/new" + idx;
      String fullFile = "tmp/full" + idx;
      recursionNodeToFilename.put(rn, deltaFile);
      compile(rn.getExitPlan(), sb, indent + INDENT, Context.PIPE);
      sb.append(" | tee " + fullFile + " > " + deltaFile + "\n");
      // "do while" loop in bash
      sb.append(indent + "while \n");

      recursionSorted(rn, sb, indent, Context.PIPE, fullFile, deltaFile, newDeltaFile);
      //recursionInMemory(rn, sb, indent, fullFile, deltaFile, newDeltaFile);

      sb.append(indent + INDENT + "[ -s " + deltaFile + " ]; \n");
      sb.append(indent + "do continue; done\n");
      //sb.append(indent + "rm " + deltaFile + "\n");
      sb.append("cat " + fullFile);
      ctx.end(Context.PIPE, sb);
    } else if (planNode instanceof DeltaNode) {
      String deltaFile = recursionNodeToFilename.get(((DeltaNode) planNode).getRecursionNode());
      if(ctx == Context.FILE) {
        sb.append(deltaFile);
      }
      else {
        ctx.start(Context.PIPE, sb);
        sb.append("cat " + deltaFile);
        ctx.end(Context.PIPE, sb);
      }
    } else {
      System.err.println("compilation of " + planNode.getClass() + " not yet supported");
    }
  }

}
