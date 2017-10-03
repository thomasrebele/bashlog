package bashlog;

import bashlog.plan.SortJoinNode;
import bashlog.plan.SortNode;
import common.parser.CompoundTerm;
import common.parser.Constant;
import common.plan.*;
import common.plan.RecursionNode.DeltaNode;

import java.util.*;

public class BashlogCompiler {

  Map<RecursionNode, String> recursionNodeToFilename = new HashMap<>();

  HashMap<PlanNode, Info> planToInfo = new HashMap<>();

  PlanNode root;

  public BashlogCompiler(PlanNode planNode) {
    root = new PlanSimplifier().apply(planNode);
    root = new PushDownFilterOptimizer().apply(root);

    //this.root = root.transform(this::transform);

    System.out.println("optimized");
    System.out.println(root.toPrettyString());

    root = root.transform(this::transform);
    //root = root.transform(this::pushSelection);

    System.out.println("bashlog plan");
    System.out.println(root.toPrettyString());
  }

  public String compile() {
    return compile("");
  }

  public String compile(String indent) {
    analyzeUsage(root);
    analyzeReuse(root, 1, new ArrayList<>(), new HashSet<>());

    StringBuilder sb = new StringBuilder();
    sb.append("# reuse common plans\n");
    sb.append("mkdir -p tmp\n");
    planToInfo.forEach((p, info) -> {
      if (info.reuse()) {
        if (!info.materialize()) {

          sb.append("mkfifo");
          for (int i = 0; i < info.planUseCount; i++) {
            sb.append(" " + info.filename + "_" + i);
          }
          sb.append("\n");
        }
          compileRaw(p, sb, indent);
        sb.append(" \\\n");
        if (info.materialize()) {
          sb.append(" > ");
          sb.append(info.filename);
        } else {
          for (int i = 0; i < info.planUseCount; i++) {
            sb.append(i < info.planUseCount - 1 ? " | tee " : " > ");
            sb.append(info.filename + "_" + i);
          }
          sb.append(" &");
        }
        sb.append("\n");
      }
      System.out.println(info + " <- " + ("" + p.hashCode()).substring(0, 3) + "  " + p);
    });
    System.out.println();

    sb.append("\n\n# main plan\n");
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
      RecursionNode recursionNode = (RecursionNode) p;
      return recursionNode.transform(
              new SortNode(recursionNode.getExitPlan(), null),
              new SortNode(recursionNode.getRecursivePlan(), null)
      );
    }

    return p;
  }

  //  private PlanNode pushSelection(PlanNode p, PlanNode[] args) {
  //    if (p instanceof ConstantEqualityFilterNode) {
  //    System.out
  //        .println(
  //            "push selection " + p.operatorString() + " args " + Arrays.stream(args).map(a -> a.toPrettyString()).collect(Collectors.joining(", ")));
  //      ConstantEqualityFilterNode s = ((ConstantEqualityFilterNode) p);
  //      // TODO: ConstantEqualityFilterNode
  //      if (args[0] instanceof UnionNode) {
  //        return new UnionNode(
  //            args[0].args().stream().map(c -> new ConstantEqualityFilterNode(c, s.getField(), s.getValue()).transform(this::pushSelection))
  //                .collect(Collectors.toSet()),
  //            s.getArity());
  //      } else if(args[0] instanceof ProjectNode) {
  //        System.out.println("trying to push over " + args[0].operatorString());
  //        ProjectNode pj = ((ProjectNode) args[0]);
  //        int[] proj = pj.getProjection();
  //        int dstCol = proj[s.getField()];
  //        if (dstCol >= 0) {
  //          return new ProjectNode(new ConstantEqualityFilterNode(pj.getTable(), dstCol, s.getValue()).transform(this::pushSelection), proj);
  //        }
  //      } else if (args[0] instanceof SortJoinNode) {
  //        SortJoinNode sjn = (SortJoinNode) args[0];
  //        boolean pushLeft = s.getField() < sjn.getLeft().getArity() || Arrays.stream(sjn.getLeftJoinProjection()).anyMatch(i -> i == s.getField());
  //        boolean pushRight = s.getField() >= sjn.getRight().getArity() || Arrays.stream(sjn.getRightJoinProjection()).anyMatch(i -> i == s.getField());
  //        
  //        System.out.println("pushing " + s);
  //        return new SortJoinNode(
  //            (pushLeft ? new ConstantEqualityFilterNode(sjn.getLeft(), s.getField(), s.getValue()).transform(this::pushSelection) : sjn.getLeft()), //
  //            (pushRight
  //                ? new ConstantEqualityFilterNode(sjn.getRight(), s.getField() - sjn.getLeft().getArity(), s.getValue()).transform(this::pushSelection)
  //                : sjn.getRight()), //
  //            sjn.getLeftJoinProjection(), sjn.getRightJoinProjection()
  //            );
  //      } else if(args[0] instanceof SortNode) {
  //        SortNode sn = (SortNode) args[0];
  //        return new SortNode(new ConstantEqualityFilterNode(sn.getTable(), s.getField(), s.getValue()).transform(this::pushSelection),
  //            sn.sortColumns());
  //      } else if (args[0] instanceof RecursionNode) {
  //        // only push to recursive plan, if selection arrives at all delta nodes (and occurs at least one more time)!
  //        RecursionNode rn = (RecursionNode) args[0];
  //        PlanNode newExit = new ConstantEqualityFilterNode(rn.getExitPlan(), s.getField(), s.getValue()).transform(this::pushSelection);
  //
  //        boolean canBePushedDown = false;
  //        class DeltaCounter {
  //
  //          int count = 0;
  //
  //          public PlanNode count(PlanNode tmpP, PlanNode[] tmpArgs) {
  //            if (tmpP instanceof DeltaNode) {
  //              count++;
  //            }
  //            tmpP = pushSelection(tmpP, tmpArgs);
  //            return tmpP; //tmpP.transform((DeltaCounter.this)::count);
  //          }
  //        }
  //
  //        DeltaCounter dc = new DeltaCounter();
  //        System.out.println("new recursive plan: ");
  //        PlanNode newRecursion; //= new ConstantEqualityFilterNode(rn.getRecursivePlan(), s.getField(), s.getValue()).transform(dc::count);
  //        newRecursion = new ConstantEqualityFilterNode(rn.getRecursivePlan(), s.getField(), s.getValue()).transform(this::pushSelection);
  //
  //        System.out.println(newRecursion.toPrettyString());
  //        System.out.println("found " + dc.count + " deltas");
  //
  //        System.exit(0);
  //        //return 
  //
  //      }
  //    } 
  //    return null;
  //  }

  /** Counts how often each subplan occurs in the tree */
  private void analyzeUsage(PlanNode p) {
    Info i = planToInfo.computeIfAbsent(p, k -> new Info(k));
    if (i.filename == null) i.filename = "tmp/relation" + planToInfo.size();
    i.planUseCount++;
    p.args().forEach(c -> analyzeUsage(c));
  }

  /**
   * Check whether subtree is used more often than their parent.
   * Plans containing delta nodes cannot be reused.
   * @param p
   * @param useCount
   * @param deltaPlans subplans of these nodes are blocked for reuse (deals with delta nodes)
   */
  private void analyzeReuse(PlanNode p, int useCount, List<PlanNode> recursions, Set<PlanNode> deltaPlans) {
    Info info = planToInfo.computeIfAbsent(p, k -> new Info(k));
    info.recursions.addAll(recursions);

    if (p instanceof DeltaNode) {
      deltaPlans.add(((DeltaNode) p).getRecursionNode());
      return;
    }

    if (info.planUseCount > useCount) {
      info.reuse = true;
      if (recursions.size() > 0) {
        info.reuse &= !deltaPlans.contains(recursions.get(recursions.size() - 1));
      }
      useCount = info.planUseCount;
    }
    List<PlanNode> children = p.args();
    for (int i = 0; i < children.size(); i++) {
      PlanNode c = children.get(i);
      if (p instanceof RecursionNode && i == 1) {
        recursions.add(p);
      }
      HashSet<PlanNode> dr = new HashSet<>();
      analyzeReuse(c, useCount, recursions, dr);
      deltaPlans.addAll(dr);
    }
    if (p instanceof RecursionNode) {
      recursions.remove(recursions.size() - 1);
      deltaPlans.remove(p);
    }
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
    sb.append(" | LC_ALL=C sort -t $'\\t' ");
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

    sb.append("LC_ALL=C join -t $'\\t' -1 ");
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

  /** Compile, reusing common subplans (including planNode) */
  private void compile(PlanNode planNode, StringBuilder sb, String indent) {
    Info info = planToInfo.get(planNode);
    if (info.reuse()) {
        sb.append("cat " + planToInfo.get(planNode).filename);
      if (!info.materialize()) {
        sb.append("_" + info.bashUseCount++);
      }
    } else {
      compileRaw(planNode, sb, indent);
    }
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
    sb.append(indent + INDENT + "LC_ALL=C sort -u --merge -o " + fullFile + " " + fullFile + " <(LC_ALL=C sort " + deltaFile + ")\n");
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

  /** Compile planNode as is, reuse its decendants */
  private void compileRaw(PlanNode planNode, StringBuilder sb, String indent) {
    if (planNode instanceof SortNode) {
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
      String deltaFile = "tmp/delta" + recursionNodeToFilename.size();
      String newDeltaFile = "tmp/new" + recursionNodeToFilename.size();
      String fullFile = "tmp/full" + recursionNodeToFilename.size();
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

  /** Statistics for reusing subplans */
  private class Info {

    PlanNode plan;

    /** Materialized table / prefix for named pipes (filename_0, filename_1, ...) */
    String filename;

    /** Count how often plan node appears in tree */
    int planUseCount = 0;

    /** Recursion ancestor nodes, which iterate over plan node */
    Set<PlanNode> recursions = new HashSet<>();

    /** Count how often a named pipe for plan node was used */
    int bashUseCount = 0;

    /** If reuse, either materialize output to file, or create named pipes and fill them with tee */
    private boolean reuse = false;

    /** Save output to file; works only if reuse == true */
    //boolean materialize = false;

    public Info(PlanNode plan) {
      this.plan = plan;
    }

    boolean reuse() {
      return reuse && !(plan instanceof BuiltinNode);
    }

    boolean materialize() {
      return reuse && recursions.size() > 0;
    }

    @Override
    public String toString() {
      return filename + " uc:" + planUseCount + " rc:" + recursions.size() + " reuse " + reuse + " mat " + materialize();
    }
  }

}
