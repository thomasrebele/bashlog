package bashlog;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bashlog.command.Bash;
import bashlog.plan.*;
import bashlog.translation.BashTranslator;
import common.parser.Program;
import common.plan.LogicalPlanBuilder;
import common.plan.node.PlanNode;
import common.plan.optimizer.*;

/**
 * Transform an extended relational algebra plan to a bash script.
 * @author Thomas Rebele
 */
public class BashlogCompiler {

  private static final Logger LOG = LoggerFactory.getLogger(BashlogCompiler.class);

  public static final Set<String> BASHLOG_PARSER_FEATURES = new HashSet<>(Arrays.asList());

  /** Query plan which should be translated */
  PlanNode root;

  /** Stores the compiled bash script */
  private String bash = null;

  /** Save debug information (query plans)*/
  private StringBuilder debugBuilder = null; //new StringBuilder();

  private String debug;

  private List<List<Optimizer>> stages = Arrays.asList(//
      Arrays.asList(new CombineFacts(), new SimplifyRecursion(), new PushDownJoin(), new ReorderJoinLinear(), new PushDownFilterAndProject(),
          new SimplifyRecursion(), new PushDownFilterAndProject()),
      Arrays.asList(new BashlogPlan(), new BashlogOptimizer(), new MultiOutput(), new CombineFilter(false), new Materialize(),
          new CombineFilter(false)));

  private Map<Class<?>, BashTranslator> translators = new HashMap<>();

  public BashlogCompiler(PlanNode planNode) {
    if (planNode == null) {
      throw new IllegalArgumentException("cannot compile an empty plan");
    }
    this.root = planNode;
  }

  private boolean isInitialized = false;

  private void init() {
    if (isInitialized) return;
    isInitialized = true;
    // register translators
    Arrays
        .asList(new bashlog.translation.BashCmd(), new bashlog.translation.CombineColumns(), new bashlog.translation.FileInput(),
            new bashlog.translation.Join(), new bashlog.translation.Materialization(), new bashlog.translation.MultiFilter(),
            new bashlog.translation.MultiOutput(), new bashlog.translation.ProjectFilter(), new bashlog.translation.Recursion(),
            new bashlog.translation.Sort(), new bashlog.translation.Union(), new bashlog.translation.Fact())
        .forEach(t -> t.supports().forEach(c -> translators.put(c, t)));

    if (debugBuilder != null) {
      debugBuilder.append("orig\n");
      debugBuilder.append(root.toPrettyString() + "\n");
    }
    root = new SortNode(root, null);

    List<String> stageNames = Arrays.asList("simplification", "optimization", "transforming to bashlog plan");
    //root = Optimizer.applyOptimizer(root, stageNames, stages, debugBuilder);
    if (debugBuilder == null) {
      root = Optimizer.applyOptimizer(root, stages);
    } else {
      try {
        root = Optimizer.applyOptimizer(root, stageNames, stages, debugBuilder);
      } catch (Exception e) {
        throw e;
      } finally {
        debug = "#" + debugBuilder.toString().replaceAll("\n", "\n# ");
      }
    }
  }

  public String compile() {
    if (bash == null) {
      bash = compile("", "", true);
    }
    return bash;
  }

  public String compile(String indent, String postCmd, boolean comments) {
    init();
    StringBuilder header = new StringBuilder();
    // we generate a bash script (shebang)
    header.append("#!/bin/bash\n");
    header.append("###############################################################\n");
    header.append("# This script was generated by bashlog\n");
    header.append("# For more information, visit thomasrebele.org/projects/bashlog\n");
    header.append("###############################################################\n\n");

    // set LC_ALL for efficiency and consistency between sort and join command
    header.append("export LC_ALL=C\n");
    // for temporary files
    header.append("mkdir -p tmp\n");
    header.append("if [ ! -z \"$(ls -A tmp)\" ]; then");
    header.append("    echo \"Directory $(pwd)/tmp/ is not empty. Please remove and re-execute the script. Aborting.\" >&2;");
    header.append("    exit 1;");
    header.append("fi\n");
    header.append("rm -f tmp/*\n");
    // use mawk if possible for better performance
    header.append("if type mawk > /dev/null; then awk=\"mawk\"; else awk=\"awk\"; fi\n");
    // tweak sort
    header.append("sort=\"sort \"\n");
    header.append("check() { grep -- $1 <(sort --help) > /dev/null; }\n");

    // count sort usage
    int sortBuffer = (int) Math.ceil(100. / Math.max(1, countConcurrentSorts(root)));

    header.append("check \"--buffer-size\" && sort=\"$sort --buffer-size=" + sortBuffer + "% \"\n");
    header.append("check \"--parallel\"    && sort=\"$sort --parallel=2 \"\n\n");

    // n-triple support
    header.append("read_ntriples() { $awk -F\" \" '{ sub(\" \", \"\\t\"); sub(\" \", \"\\t\"); sub(/ \\.$/, \"\"); print $0 }' \"$@\"; }\n");
    header.append("conv_ntriples() { $awk -F$'\\t' '{ print $1 \" \" $2 \" \" $3 \" .\" }'; }\n\n");

    header.append("unlock() {\n");
    header.append("    mv \"$1\" \"$1_done\";\n");
    header.append("    (cat \"$1_done\" > /dev/null; rm \"$1_done\") &\n");
    header.append("    while [ -p \"$1_done\" ]; do exec 3> \"$1_done\"; exec 3>&-; done\n");
    header.append("}\n\n\n");

    CompilerInternals bc = new CompilerInternals(translators, root);
    Bash e = bc.compile(root);
    String result = header.toString() + e.generate() + postCmd + "\n\n rm -f tmp/*\n";

    return result;
  }

  private int countConcurrentSorts(PlanNode p) {
    if (p instanceof SortNode) {
      return Math.max(1, countConcurrentSorts(((SortNode) p).getTable()));
    }
    return p.children().stream().mapToInt(c -> countConcurrentSorts(c)).sum();
  }

  public String debugInfo() {
    return debug;
  }

  /** Transform datalog program and query relation to a bash script. */
  public static String compileQuery(Program p, String query) throws IOException {
    BashlogCompiler bc = prepareQuery(p, query);
    try {
      String bash = bc.compile("", "", false);
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

    String relation = p.searchRelation(query);
    if (relation == null) throw new IllegalArgumentException("relation not found");
    SortedMap<String, PlanNode> plan = new LogicalPlanBuilder(builtin, Collections.singleton(relation)).getPlanForProgram(p);

    BashlogCompiler bc = new BashlogCompiler(plan.get(relation));
    return bc;
  }

  public void enableDebug() {
    if (this.debugBuilder == null) {
      this.debugBuilder = new StringBuilder();
    }
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
