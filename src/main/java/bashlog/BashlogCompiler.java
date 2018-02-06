package bashlog;

import bashlog.command.Bash;
import bashlog.plan.*;
import bashlog.translation.AwkHelper;
import bashlog.translation.Translator;
import common.Tools;
import common.parser.Constant;
import common.parser.ParserReader;
import common.parser.Program;
import common.plan.LogicalPlanBuilder;
import common.plan.node.*;
import common.plan.optimizer.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author Thomas Rebele
 */
public class BashlogCompiler {

  private static final Logger LOG = LoggerFactory.getLogger(BashlogCompiler.class);

  final static String INDENT = "    ";

  /** Current index for temporary files. Increment when using it! */
  int tmpFileIndex = 0;

  /** Maps a materialization node to its temporary file. Reuse nodes use the filename of the materialized relation. */
  Map<PlaceholderNode, String> placeholderToFilename = new HashMap<>();

  /** Query plan which should be translated */
  PlanNode root;

  Map<PlanNode, Bash> cache = new HashMap<>();
  
  /** Save debug information (query plans)*/
  private String debug = "";

  private boolean parallelMaterialization = true;

  /** Stores the compiled bash script */
  private String bash = null;

  private List<List<Optimizer>> stages = Arrays.asList(//
      Arrays.asList(new SimplifyRecursion(), new PushDownJoin(), new ReorderJoinLinear(), new PushDownFilterAndProject(), new SimplifyRecursion(),
          new PushDownFilterAndProject()),
      Arrays.asList(r -> r.transform(this::transform), new BashlogOptimizer(), new MultiOutput(), new CombineFilter(false), new Materialize(),
          new CombineFilter(false)));
  
  private Map<Class<?>, Translator> translators = new HashMap<>();

  
  
  public BashlogCompiler(PlanNode planNode) {
    if (planNode == null) {
      throw new IllegalArgumentException("cannot compile an empty plan");
    }
    
    // register translators
    Arrays.asList(
        new bashlog.translation.Builtin(),
        new bashlog.translation.CombineColumns(),
        new bashlog.translation.FileInput(),
        new bashlog.translation.Join(),
        new bashlog.translation.MultiFilter(),
        new bashlog.translation.ProjectFilter(),
        new bashlog.translation.Sort(),
        new bashlog.translation.Union()
    ).forEach(t -> t.supports().forEach(c -> translators.put(c, t)));
    
    
    root = planNode;
    debug += "orig\n";
    debug += root.toPrettyString() + "\n";
    root = new SortNode(root, null);

    List<String> stageNames = Arrays.asList("simplification", "optimization", "transforming to bashlog plan");
    Iterator<String> it = stageNames.iterator();
    PlanValidator check = new PlanValidator();
    for (List<Optimizer> stage : stages) {
      debug += "\n\n" + (it.hasNext() ? it.next() : "") + "\n";
      for (Optimizer o : stage) {
        root = o.apply(root);

        debug += "applied " + o.getClass() + " \n";
        debug += root.toPrettyString() + "\n";

        try {
          check.apply(root);
        } catch (Exception e) {
          LOG.error(e.getMessage());
          debug += "WARNING: " + e.getMessage();
        }
      }
    }
    debug = "#" + debug.replaceAll("\n", "\n# ");
  }

  public String compile() {
    if (bash == null) {
      bash = compile("", true);
    }
    return bash;
  }

  public String compile(String indent, boolean comments) {
    StringBuilder header = new StringBuilder();
    // we generate a bash script (shebang)
    header.append("#!/bin/bash\n");
    // set LC_ALL for efficiency and consistency between sort and join command
    header.append("export LC_ALL=C\n");
    // for temporary files
    header.append("mkdir -p tmp\n");
    header.append("rm tmp/*\n");
    // use mawk if possible for better performance
    header.append("if type mawk > /dev/null; then awk=\"mawk\"; else awk=\"awk\"; fi\n");
    // tweak sort
    header.append("sort=\"sort -S25% --parallel=2 \"\n\n");

    Bash e = compile(root);
    String result = header.toString() + e.generate();

    return result;
  }

  public String debugInfo() {
    return debug;
  }

  /** Adds extra column with dummy value */
  private PlanNode prepareSortCrossProduct(PlanNode p) {
    int[] proj = new int[p.getArity() + 1];
    Comparable<?>[] cnst = new Comparable[proj.length];
    for (int i = 0; i < p.getArity(); i++) {
      proj[i + 1] = i;
    }
    proj[0] = -1;
    cnst[0] = "_";
    return p.project(proj, cnst);
  }

  private PlanNode prepareSortJoin(PlanNode p, int[] columns) {
    if (columns.length == 1) {
      return new SortNode(p, columns);
    }
    CombinedColumnNode c = new CombinedColumnNode(p, columns);
    return new SortNode(c, new int[] { p.getArity() });
  }

  /** Replace certain common.plan.* nodes with their bashlog implementations */
  private PlanNode transform(PlanNode p) {
    if (p instanceof JoinNode) {
      // replace join node with sort join node
      JoinNode joinNode = (JoinNode) p;
      if (joinNode.getLeftProjection().length == 0) {
        // no join condition, so do a cross product
        // sort input and add a dummy column
        PlanNode left = prepareSortCrossProduct(joinNode.getLeft());
        PlanNode right = prepareSortCrossProduct(joinNode.getRight());
        PlanNode crossProduct = new SortJoinNode(left, right, new int[] { 0 }, new int[] { 0 });

        // remove extra columns
        int[] proj = new int[left.getArity() + right.getArity() - 2];
        for (int i = 1; i < left.getArity(); i++) {
          proj[i - 1] = i;
        }
        for (int i = 1; i < right.getArity(); i++) {
          proj[left.getArity() - 2 + i] = left.getArity() + i;
        }
        return crossProduct.project(proj);
      } else {
        // sort input and add combined column if necessary
        PlanNode left = prepareSortJoin(joinNode.getLeft(), joinNode.getLeftProjection());
        PlanNode right = prepareSortJoin(joinNode.getRight(), joinNode.getRightProjection());
        if (joinNode.getLeftProjection().length == 1) {
          // no combined column necessary, so we can directly return the join
          return new SortJoinNode(left, right, joinNode.getLeftProjection(), joinNode.getRightProjection());
        }
        // remove extra columns
        PlanNode join = new SortJoinNode(left, right, new int[] { left.getArity() - 1 }, new int[] { right.getArity() - 1 });
        int rightStart = left.getArity();
        return join.project(Tools.concat(Tools.sequence(left.getArity() - 1), Tools.sequence(rightStart, rightStart + right.getArity() - 1)));
      }

    } else if (p instanceof AntiJoinNode) {
      AntiJoinNode ajn = (AntiJoinNode) p;
      PlanNode left = prepareSortJoin(ajn.getLeft(), ajn.getLeftProjection());
      PlanNode right = prepareSortJoin(ajn.getRight(), Tools.sequence(ajn.getRight().getArity()));

      if (ajn.getLeftProjection().length == 1) {
        // no combined column necessary, so we can directly return the join
        return new SortAntiJoinNode(left, right, ajn.getLeftProjection());
      }
      // remove extra columns
      PlanNode antijoin = new SortAntiJoinNode(left, right.project(new int[] { right.getArity() - 1 }), new int[] { left.getArity() - 1 });
      return antijoin.project(Tools.sequence(left.getArity() - 1));

    } else if (p instanceof RecursionNode) {
      // use sorted recursion
      RecursionNode r = (RecursionNode) p;
      return new RecursionNode(new SortNode(r.getExitPlan(), null), new SortNode(r.getRecursivePlan(), null), r.getDelta(), r.getFull());

    } else if (p instanceof UnionNode) {
      UnionNode u = (UnionNode) p;
      List<PlanNode> children = u.children();
      if (children.size() == 0) return u;
      if (children.size() == 1) return children.get(0);
      return u;

    } else if (p instanceof BuiltinNode) {
      // instead of "<(cat file)", use file directly
      BuiltinNode b = (BuiltinNode) p;
      if ("bash_command".equals(b.compoundTerm.name)) {
        String cmd = (String) ((Constant<?>) b.compoundTerm.args[0]).getValue();
        if (cmd.startsWith("cat ")) {
          ParserReader pr = new ParserReader(cmd);
          pr.expect("cat ");
          pr.skipWhitespace();
          String file;
          if (pr.peek() == '\"' || pr.peek() == '\'') file = pr.readString();
          else file = pr.readWhile((c, s) -> !Character.isWhitespace(c));
          pr.skipWhitespace();
          if (pr.peek() == '\0') {
            return new TSVFileNode(file, p.getArity());
          }
        }
      }
      // check whether bashlog supports builtin predicate b is done in compile(...)
    }

    return p;
  }

  private Bash setMinusSorted(Bash prev, String filename) {
    Bash.Pipe result = prev.pipe();
    result.cmd("comm")//
        .arg("--nocheck-order").arg("-23").arg("-")//
        .file(filename);
    return result;
  }

  private Bash recursionSorted(RecursionNode rn, String fullFile, String deltaFile, String newDeltaFile) {
    Bash prev = compile(rn.getRecursivePlan());
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

  Bash waitFor(Bash bash, List<PlanNode> children) {
    Bash result = bash;
    if (parallelMaterialization) {
      for (PlanNode child : children) {
        if (child instanceof PlaceholderNode) {
          PlanNode parent = ((PlaceholderNode) child).getParent();
          if (parent instanceof MaterializationNode) {
            String matFile = placeholderToFilename.get(child);
             if (parallelMaterialization ) {
              result = new Bash.Command("cat").arg(matFile.replace("tmp/", "tmp/lock_")).arg("1>&2").arg("; ").other(result);
            }
          }
        }
      }
    }
    return result;
  }

   public Bash compile(PlanNode planNode) {
    return waitFor(compileIntern(planNode), planNode.children());
  }

  /**
   * @param planNode
   * @return
   */
  private Bash compileIntern(PlanNode planNode) {
    if(cache.containsKey(planNode)) return cache.get(planNode);
    Translator t = translators.get(planNode.getClass());
    if(t != null) {
      return t.translate(planNode, this);
    }
    
    if (planNode instanceof MaterializationNode) {
      MaterializationNode m = (MaterializationNode) planNode;
      String matFile = "tmp/mat" + tmpFileIndex++;
      placeholderToFilename.putIfAbsent(m.getReuseNode(), matFile);
      Bash.CommandSequence result = new Bash.CommandSequence();
      result.comment(planNode, "");
      result.info(planNode, "");

      Bash reused = compile(m.getReusedPlan());
        if (parallelMaterialization ) {
          String lockFile = matFile.replaceAll("tmp/", "tmp/lock_");
          String doneFile = matFile.replaceAll("tmp/", "tmp/done_");
          reused = reused.wrap("mkfifo " + lockFile + "; ( ", //
              " > " + matFile + //
                  "; mv " + lockFile + " " + doneFile + //
                  "; cat " + doneFile + " > /dev/null & " + //
                  "exec 3> " + doneFile + "; exec 3>&-;" + //
                  " ) & ");
        } else {
          reused = reused.wrap("", " > " + matFile);
        }
        result.add(reused);

      if (!(m.getMainPlan() instanceof MaterializationNode)) {
        result.other("\n# plan");
      }
      result.add(compile(m.getMainPlan()));
      return result;

    } else if (planNode instanceof MultiOutputNode) {
      Bash.CommandSequence result = new Bash.CommandSequence();
      Bash.Command touch = result.cmd("touch");
      Bash.Command cmd = result.cmd(AwkHelper.AWK);
      MultiOutputNode mo = (MultiOutputNode) planNode;

      StringBuilder arg = new StringBuilder();
      List<PlanNode> plans = mo.reusedPlans(), nodes = mo.reuseNodes();
      for (int i = 0; i < plans.size(); i++) {
        PlanNode plan = plans.get(i), node = nodes.get(i);

        String matFile = "tmp/mat" + tmpFileIndex++;
        touch.file(matFile);
        placeholderToFilename.putIfAbsent((PlaceholderNode) node, matFile);

        //TODO: if there are more conditions on one output file:
        // if (!complexAwkLine(Arrays.asList(plan), matFile, arg).isEmpty()) { ... }
        AwkHelper.simpleAwkLine(plan, matFile, arg);
      }
      cmd.arg(arg.toString()).arg("'");
      cmd.file(compile(mo.getLeaf()));
      result.add(compile(mo.getMainPlan()));
      return result;

    } else if (planNode instanceof RecursionNode) {
      RecursionNode rn = (RecursionNode) planNode;
      int idx = tmpFileIndex++;
      String deltaFile = "tmp/delta" + idx;
      String newDeltaFile = "tmp/new" + idx;
      String fullFile = "tmp/full" + idx;
      placeholderToFilename.put(rn.getDelta(), deltaFile);
      placeholderToFilename.put(rn.getFull(), fullFile);

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
      result.cmd("cat").file(fullFile);
      return result;

    } else if (planNode instanceof PlaceholderNode) {
      PlanNode parent = ((PlaceholderNode) planNode).getParent();
      String file = placeholderToFilename.get(planNode);
      if (file == null) {
        placeholderToFilename.forEach((m, f) -> System.err.println(m.operatorString() + "  " + f));
        throw new IllegalStateException("no file assigned to " + planNode.operatorString() + " for " + parent.operatorString());
      }
      return new Bash.BashFile(file);
    }
    // fallback
    throw new UnsupportedOperationException("compilation of " + planNode.getClass() + " not yet supported");
  }

  /**
   * Get projection array, and columns and constants for filters
   * @param node
   * @param projCols accumulator
   * @param filterCols accumulator
   * @return inner plan node
   */
  private PlanNode getCols(PlanNode node, List<Integer> projCols, Map<Integer, Comparable<?>> filterCols) {
    if (node instanceof ConstantEqualityFilterNode) {
      ConstantEqualityFilterNode eq = (ConstantEqualityFilterNode) node;
      filterCols.put(eq.getField(), eq.getValue());
      return getCols(eq.getTable(), projCols, filterCols);
    }
    if (node instanceof VariableEqualityFilterNode) {
      // make getCols return null (checked below)
      return null;
    }
    if (node instanceof ProjectNode) {
      // may only have one projection!
      if (!projCols.isEmpty()) throw new UnsupportedOperationException(((ProjectNode) node).getTable().toString());
      ProjectNode p = (ProjectNode) node;
      Arrays.stream(p.getProjection()).forEach(i -> projCols.add(i));
      return getCols(p.getTable(), projCols, filterCols);
    }
    return null;
  }

  /** Transform datalog program and query relation to a bash script. */
  public static String compileQuery(Program p, String query) throws IOException {
    BashlogCompiler bc = prepareQuery(p, query);
    try {
      String bash = bc.compile("", false);
      return bash + "\n\n"; //+ bc.debugInfo();
    } catch (Exception e) {
      LOG.error(bc.debugInfo());
      throw (e);
    }
  }

  /** Initialize bashlog compiler with program and query relation. */
  public static BashlogCompiler prepareQuery(Program p, String query) {
    Set<String> builtin = new HashSet<>();
    builtin.add("bash_command");
    Map<String, PlanNode> plan = new LogicalPlanBuilder(builtin).getPlanForProgram(p);

    PlanNode pn = plan.get(query);
    BashlogCompiler bc = new BashlogCompiler(pn);
    return bc;
  }

  /*public static void main(String[] args) {
    PlanNode table = new TSVFileNode("abc", 5);
    MultiFilterNode mfn = new MultiFilterNode(new HashSet<>(Arrays.asList(
        //table.equalityFilter(1, 2).project(new int[] { 1, 2 }), //
        table.equalityFilter(3, "abc").project(new int[] { 1, 2 }), //
        table.equalityFilter(3, "def").project(new int[] { 1, 2 }), //
        table.equalityFilter(3, "ghi").project(new int[] { 1, 2 }), //
        table.equalityFilter(1, 2).project(new int[] { 3, 4 }))), table, 2);
  
    BashlogCompiler bc = new BashlogCompiler(mfn);
    Bash b = bc.compileIntern(mfn);
    System.out.println(b.generate());
  }*/

}
