package bashlog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import bashlog.command.Bash;
import bashlog.translation.Translator;
import common.plan.node.MaterializationNode;
import common.plan.node.PlaceholderNode;
import common.plan.node.PlanNode;

public class CompilerInternals {

  private final Map<Class<?>, Translator> translators;

  /** Current index for temporary files. Increment when using it! */
  AtomicInteger tmpFileIndex = new AtomicInteger();

  /** Maps a materialization node to its temporary file. Reuse nodes use the filename of the materialized relation. */
  Map<PlaceholderNode, String> placeholderToFilename = new HashMap<>();

  private boolean parallelMaterialization = true;

  Map<PlanNode, Bash> cache = new HashMap<>();

  CompilerInternals(Map<Class<?>, Translator> translators) {
    this.translators = translators;
  }

  public void registerPlaceholder(PlaceholderNode node, String file) {
    placeholderToFilename.put(node, file);
  }

  public boolean parallelMaterialization() {
    return parallelMaterialization;
  }

  /** Next index for temporary files (materialized, delta, full) */
  public int getNextIndex() {
    return tmpFileIndex.getAndIncrement();
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
      for (PlanNode child : children) {
        if (child instanceof PlaceholderNode) {
          PlanNode parent = ((PlaceholderNode) child).getParent();
          if (parent instanceof MaterializationNode) {
            String matFile = placeholderToFilename.get(child);
            if (parallelMaterialization) {
              result = new Bash.Command("cat").arg(matFile.replace("tmp/", "tmp/lock_")).arg("1>&2").arg("; ").other(result);
            }
          }
        }
      }
    }
    return result;
  }

  /**
   * Translate a relational algebra plan to a bash snippet
   * @param planNode
   * @return
   */
  public Bash compile(PlanNode planNode) {
    if (cache.containsKey(planNode)) return cache.get(planNode);

    // apply corresponding translator if possible; also applies locking for parallel materialization
    Translator t = translators.get(planNode.getClass());
    if (t != null) {
      return waitFor(t.translate(planNode, this), planNode.children());
    }

    // inject files for placeholder nodes
    if (planNode instanceof PlaceholderNode) {
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
}
