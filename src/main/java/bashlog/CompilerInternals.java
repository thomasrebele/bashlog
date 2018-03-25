package bashlog;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import bashlog.command.Bash;
import bashlog.translation.BashTranslator;
import common.plan.node.MaterializationNode;
import common.plan.node.PlaceholderNode;
import common.plan.node.PlanNode;

/** Stores information that is needed during the translation. For the translation you need to use BashlogCompiler. */
public class CompilerInternals extends common.compiler.CompilerInternals<BashTranslator> {

  /** Current index for temporary files. Increment when using it! */
  AtomicInteger tmpFileIndex = new AtomicInteger();

  /** Maps a materialization node to its temporary file. Reuse nodes use the filename of the materialized relation. */
  Map<PlaceholderNode, String> placeholderToFilename = new HashMap<>();

  Map<String, PlaceholderNode> filenameToPlaceholder = new HashMap<>();

  private boolean parallelMaterialization = true;

  Map<PlanNode, Bash> cache = new HashMap<>();

  /** 
   * Constructor
   * @param translators map from a node class to its translator
   */
  CompilerInternals(Map<Class<?>, bashlog.translation.BashTranslator> translators, PlanNode fullPlan) {
    super(translators, fullPlan);

  }

  /** Whether the resulting bash script will materialize multiple plans in parallel */
  public boolean parallelMaterialization() {
    return parallelMaterialization;
  }

  /** Indicates that plan *node* should take its input from *file* */
  public void registerPlaceholder(PlaceholderNode node, String file) {
    placeholderToFilename.put(node, file);
    filenameToPlaceholder.put(file, node);
  }

  /** Next index for temporary files (materialized, delta, full) */
  public int getNextIndex() {
    return tmpFileIndex.getAndIncrement();
  }

  /**
   * Get placeholder descendants. Recursively for all children that were not translated.
   * @param children
   * @param accumulator
   */
  private void directPlaceholderDescendants(List<PlanNode> children, Set<PlanNode> accumulator) {
    for (PlanNode c : children) {
      if (c instanceof PlaceholderNode) {
        accumulator.add(c);
      } else if (!cache.containsKey(c)) {
        directPlaceholderDescendants(c.children(), accumulator);
      }
    }
  }

  /**
   * Add a locking mechanism for materialization nodes.
   * This happens if the bash snippet directly uses a materialized file.
   * @param snippet
   * @param children
   * @return
   */
  Bash waitFor(Bash snippet, List<PlanNode> children) {
    Bash result = snippet;
    if (parallelMaterialization) {
      // We need to wait if the command in snippet uses a materialized node directly
      // Indirectly used files are treated at their respective snippet
      // Some commands (e.g. ProjectFilter) might use more than one level of the plan tree, 
      // and we need to check all files that are used by that snippet
      HashSet<PlanNode> todo = new HashSet<>();
      directPlaceholderDescendants(children, todo);

      for (PlanNode child : todo) {
        PlanNode parent = getParent((PlaceholderNode) child);
        if (parent instanceof MaterializationNode) {
          String matFile = placeholderToFilename.get(child);
          if (parallelMaterialization) {
            result = new Bash.Command("cat").arg(matFile.replace("tmp/", "tmp/lock_")).arg("1>&2").arg("2>/dev/null").arg("; ").other(result);
          }
        }
      }
    }
    return result;
  }

  /**
   * Translate a relational algebra plan to a bash snippet
   * @param planNode
   * @return bash snippet
   */
  public Bash compile(PlanNode planNode) {
    if (cache.containsKey(planNode)) return cache.get(planNode);

    // apply corresponding translator if possible; also applies locking for parallel materialization
    bashlog.translation.BashTranslator t = translators.get(planNode.getClass());
    if (t != null) {
      Bash result = waitFor(t.translate(planNode, this), planNode.children());
      cache.put(planNode, result);
      return result;
    }

    // inject files for placeholder nodes
    if (planNode instanceof PlaceholderNode) {
      PlanNode parent = getParent((PlaceholderNode) planNode);
      String file = placeholderToFilename.get(planNode);
      if (file == null) {
        placeholderToFilename.forEach((m, f) -> System.err.println(m.operatorString() + "  " + f));
        throw new IllegalStateException("no file assigned to " + planNode.operatorString() + " for " + parent.operatorString());
      }
      Bash result = new Bash.BashFile(file);
      cache.put(planNode, result);
      return result;
    }
    // fallback
    throw new UnsupportedOperationException("compilation of " + planNode.getClass() + " not yet supported");
  }
}
