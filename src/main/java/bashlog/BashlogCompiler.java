package bashlog;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bashlog.command.Bash;
import bashlog.plan.BashlogOptimizer;
import bashlog.plan.BashlogPlan;
import bashlog.plan.SortNode;
import bashlog.translation.Translator;
import common.parser.Program;
import common.plan.LogicalPlanBuilder;
import common.plan.node.*;
import common.plan.optimizer.*;

/**
 * Transform an extended relational algebra plan to a bash script.
 * @author Thomas Rebele
 */
public class BashlogCompiler {

  private static final Logger LOG = LoggerFactory.getLogger(BashlogCompiler.class);

  final static String INDENT = "    ";

  /** Query plan which should be translated */
  PlanNode root;

  /** Stores the compiled bash script */
  private String bash = null;

  /** Save debug information (query plans)*/
  private String debug = "";

  private List<List<Optimizer>> stages = Arrays.asList(//
      Arrays.asList(new SimplifyRecursion(), new PushDownJoin(), new ReorderJoinLinear(), new PushDownFilterAndProject(), new SimplifyRecursion(),
          new PushDownFilterAndProject()),
      Arrays.asList(new BashlogPlan(), new BashlogOptimizer(), new MultiOutput(), new CombineFilter(false), new Materialize(),
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
        new bashlog.translation.Materialization(),
        new bashlog.translation.MultiFilter(),
        new bashlog.translation.MultiOutput(),
        new bashlog.translation.ProjectFilter(),
        new bashlog.translation.Recursion(),
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

    CompilerInternals bc = new CompilerInternals(translators);
    Bash e = bc.compile(root);
    String result = header.toString() + e.generate() + "; rm tmp/*\n";

    return result;
  }

  public String debugInfo() {
    return debug;
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
