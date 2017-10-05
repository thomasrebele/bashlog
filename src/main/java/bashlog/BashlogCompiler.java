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

  public static class Context {

    private final int FILE = 1, PIPE = 0;
    
    private int ctx = PIPE;

    private final StringBuilder sb;

    private String indent;

    public Context(Context context, int nextContext) {
      this.ctx = nextContext;
      this.sb = context.sb;
      if (context.ctx != nextContext) {
        this.indent = context.indent + INDENT;
      } else {
        this.indent = context.indent;
      }
    }

    public Context() {
      indent = "";
      this.sb = new StringBuilder();
    }

    public Context indent() {
      Context r = new Context(this, this.ctx);
      r.indent += INDENT;
      return r;
    }

    public Context pipe() {
      return new Context(this, PIPE);
    }

    public Context file() {
      return new Context(this, FILE);
    }

    public boolean isFile() {
      return ctx == FILE;
    }

    public Context append(String string) {
      sb.append(string.replaceAll("\n", "\n" + indent));
      return this;
    }

    public Context append(Object o) {
      sb.append(o.toString());
      return this;
    }

    public String generate() {
      return sb.toString();
    }

    public void startPipe() {
      if (ctx == FILE) {
        sb.append(" \\\n").append(indent).append("<( ");
      }
    }

    public void endPipe() {
      if (ctx == FILE) {
        sb.append(")");
      }
    }

    /** Use only for debugging */
    public void info(PlanNode planNode) {
      info(planNode, "");
    }

    public void info(PlanNode planNode, String string) {
      append(" `#_________ ").append(planNode.operatorString()).append(" ").append(string).append(" _________` \\\n");

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
    Context ctx = new Context();
    ctx.append("#!/bin/bash\n");
    ctx.append("export LC_ALL=C\n");
    ctx.append("mkdir -p tmp\n");

    ctx.append("\n#test\n");
    compile(root, ctx);
    return ctx.generate();
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
    sb.append(" { ");
    sb.append("key = ");
    sb.append(keyMask(j.getRightJoinProjection()));
    sb.append("; if (key in h) {");
    sb.append(" print h[key] FS $0");
    sb.append(" } }' \\\n");
    compile(j.getLeft(), ctx.file());
    sb.append(" \\\n");
    compile(j.getRight(), ctx.file());
  }

  /**
   * Sort the output of n on columns; might need to introduce a new column
   * @param s
   * @param ctx
   * @return column which is sorted (1-based index)
   */
  private void sort(SortNode s, Context ctx) {
    int[] cols = s.sortColumns();
    ctx.startPipe();
    compile(s.args().get(0), ctx.pipe());
    ctx.append(" \\\n");
    ctx.info(s);
    if (cols != null && cols.length > 1) {
      ctx.append(" | " + AWK + "{ print $0 FS ");
      for (int i = 0; i < cols.length; i++) {
        if (i > 0) {
          ctx.append(",");
        }
        ctx.append("$");
        ctx.append(cols[i] + 1);
      }
      ctx.append("}'");
    }
    ctx.append(" | sort -t $'\\t' ");
    if (cols != null) {
      ctx.append("-k ");
      ctx.append(sortCol(s, cols));
    }
    ctx.endPipe();
  }

  private int sortCol(PlanNode n, int[] cols) {
    if (cols.length > 1) {
      return n.getArity() + 1;
    }
    return cols[0] + 1;
  }

  /** Sort left and right tree, and join with 'join' command */
  private void sortJoin(SortJoinNode j, Context ctx) {
    ctx.startPipe();
    int colLeft, colRight;
    colLeft = sortCol(j.getLeft(), j.getLeftJoinProjection());
    colRight = sortCol(j.getRight(), j.getRightJoinProjection());

    ctx.append("join -t $'\\t' -1 ");
    ctx.append(colLeft);
    ctx.append(" -2 ");
    ctx.append(colRight);
    ctx.append(" -o ");
    for (int i = 0; i < j.getLeft().getArity(); i++) {
      if (i > 0) ctx.append(",");
      ctx.append("1." + (i + 1));
    }
    ctx.append(",");
    for (int i = 0; i < j.getRight().getArity(); i++) {
      if (i > 0) ctx.append(",");
      ctx.append("2." + (i + 1));
    }
    ctx.append(" ");
    compile(j.getLeft(), ctx.file());
    compile(j.getRight(), ctx.file());
    ctx.append("");
    ctx.endPipe();
  }

  /** Remove all lines from pipe that occur in filename */
  private void setMinusInMemory(String filename, Context ctx) {
    ctx.append(" | grep -v -F -f ");
    ctx.append(filename);
  }

  private void setMinusSorted(String filename, Context ctx) {
    ctx.append(" | comm --nocheck-order -23 - ");
    ctx.append(filename);
  }

  private void recursionSorted(RecursionNode rn, Context ctx, String fullFile, String deltaFile, String newDeltaFile) {
    ctx.startPipe();
    compile(rn.getRecursivePlan(), ctx.pipe());
    ctx.append(" \\\n" + INDENT);
    //setMinusInMemory(fullFile, sb);
    setMinusSorted(fullFile, ctx);
    ctx.append(" > " + newDeltaFile + "\n");

    ctx.info(rn, "continued");
    ctx.append(INDENT + "mv " + newDeltaFile + " " + deltaFile + "; \n");
    ctx.append(INDENT + "sort -u --merge -o " + fullFile + " " + fullFile + " <(sort " + deltaFile + ")\n");
    ctx.endPipe();
  }

  private void recursionInMemory(RecursionNode rn, Context ctx, String fullFile, String deltaFile, String newDeltaFile) {
    ctx.startPipe();
    compile(rn.getRecursivePlan(), ctx.pipe());
    ctx.append(" \\\n");
    ctx.append(INDENT);
    setMinusInMemory(fullFile, ctx);
    ctx.append(" | tee -a " + fullFile);
    ctx.append(" > " + newDeltaFile + "\n");
    ctx.append(INDENT + "mv " + newDeltaFile + " " + deltaFile + "; \n");
    ctx.endPipe();
  }

  private void compile(PlanNode planNode, Context ctx) {

    if (planNode instanceof MaterializationNode) {
      ctx.startPipe();
      MaterializationNode m = (MaterializationNode) planNode;
      String matFile = "tmp/mat" + tmpFileIndex++;
      matNodeToFilename.putIfAbsent(m, matFile);

      boolean asFile = true;
      ctx.append("\n");
      ctx.info(planNode);
      /*if (!asFile) {
        sb.append("mkfifo");
        for (int i = 0; i < info.planUseCount; i++) {
          sb.append(" " + info.filename + "_" + i);
        }
        sb.append("\n");
      }*/
      compile(m.getReusedPlan(), ctx.pipe());
      ctx.append(" \\\n");
      if (asFile) {
        ctx.append(" > ");
        ctx.append(matFile);
      } else {
        /*for (int i = 0; i < info.planUseCount; i++) {
          sb.append(i < info.planUseCount - 1 ? " | tee " : " > ");
          sb.append(info.filename + "_" + i);
        }
        sb.append(" &");*/
        throw new UnsupportedOperationException("pipes not yet supported");
      }
      ctx.append("\n");
      
      if (!(m.getMainPlan() instanceof MaterializationNode)) {
        ctx.append("# plan\n");
      }
      compile(m.getMainPlan(), ctx.pipe());
      ctx.endPipe();

    } else if (planNode instanceof ReuseNode) {
      ctx.info(planNode);
      String matFile = matNodeToFilename.get(((ReuseNode) planNode).getMaterializeNode());
      if (ctx.isFile()) {
        ctx.append(matFile);
      }
      else {
        ctx.startPipe();
        ctx.append("cat " + matFile);
        ctx.endPipe();
      }
      // TODO: pipes
      /*if (!info.materialize()) {
        sb.append("_" + info.bashUseCount++);
      }*/

    } else if (planNode instanceof SortNode) {
      SortNode s = (SortNode) planNode;
      sort(s, ctx);
    } else if (planNode instanceof SortJoinNode) {
      ctx.info(planNode);
      SortJoinNode j = (SortJoinNode) planNode;
      //leftHashJoin(j, sb, indent);
      sortJoin(j, ctx);
    } else if (planNode instanceof ProjectNode) {
      // TODO: filtering of duplicates might be necessary
      ctx.startPipe();
      ProjectNode p = ((ProjectNode) planNode);
      compile(p.getTable(), ctx.pipe());
      ctx.append(" \\\n");
      ctx.info(planNode);
      ctx.append(" | " + AWK + "{ print ");
      for (int i = 0; i < p.getProjection().length; i++) {
        if (i != 0) ctx.append(" FS ");
        if (p.getProjection()[i] >= 0) {
          ctx.append("$");
          ctx.append(p.getProjection()[i] + 1);
        } else {
          p.getConstant(i).ifPresent(cnst -> ctx.append("\"" + escape(cnst.toString()) + "\""));
        }
      }
      ctx.append("}'");
      ctx.endPipe();
    } else if (planNode instanceof ConstantEqualityFilterNode) {
      ctx.startPipe();
      ConstantEqualityFilterNode n = (ConstantEqualityFilterNode) planNode;
      ctx.append(AWK + "$");
      ctx.append(n.getField() + 1);
      ctx.append(" == \"");
      ctx.append(escape(n.getValue().toString()));
      ctx.append("\" { print $0 }' ");
      compile(n.getTable(), ctx.file());
      ctx.endPipe();
    } else if (planNode instanceof BuiltinNode) {
      CompoundTerm ct = ((BuiltinNode) planNode).compoundTerm;
      if ("bash_command".equals(ct.name)) {
        ctx.startPipe();
        ctx.append(((Constant) ct.args[0]).getValue());
        ctx.endPipe();
      }
      else {
        throw new UnsupportedOperationException("predicate not supported: " + ct.getRelation());
      }
    } else if (planNode instanceof SortUnionNode) {
      ctx.startPipe();
      // delimit columns by null character
      ctx.append("comm --output-delimiter=$'\\0' ");
      for (PlanNode child : ((UnionNode) planNode).getChildren()) {
        compile(child, ctx.file());
      }
      // remove null character
      ctx.append(" | sed -E 's/^\\x0\\x0?//g'"); // 
      ctx.endPipe();
    } else if (planNode instanceof SortRecursionNode) {
      ctx.startPipe();
      ctx.info(planNode);
      RecursionNode rn = (RecursionNode) planNode;
      int idx = tmpFileIndex++;
      String deltaFile = "tmp/delta" + idx;
      String newDeltaFile = "tmp/new" + idx;
      String fullFile = "tmp/full" + idx;
      recursionNodeToFilename.put(rn, deltaFile);
      compile(rn.getExitPlan(), ctx.pipe());
      ctx.append(" | tee " + fullFile + " > " + deltaFile + "\n");
      // "do while" loop in bash
      ctx.indent().append("while \n");

      recursionSorted(rn, ctx.pipe(), fullFile, deltaFile, newDeltaFile);
      //recursionInMemory(rn, sb, indent, fullFile, deltaFile, newDeltaFile);
      ctx.append("[ -s " + deltaFile + " ]; \n");
      ctx.append("do continue; done\n");
      //sb.append(indent + "rm " + deltaFile + "\n");
      ctx.append("cat " + fullFile);
      ctx.endPipe();
    } else if (planNode instanceof DeltaNode) {
      String deltaFile = recursionNodeToFilename.get(((DeltaNode) planNode).getRecursionNode());
      if (ctx.isFile()) {
        ctx.append(deltaFile);
      }
      else {
        ctx.startPipe();
        ctx.append("cat " + deltaFile);
        ctx.endPipe();
      }
    } else {
      System.err.println("compilation of " + planNode.getClass() + " not yet supported");
    }
  }

}
