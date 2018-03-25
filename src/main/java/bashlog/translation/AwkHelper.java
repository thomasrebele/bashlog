package bashlog.translation;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import common.plan.node.ConstantEqualityFilterNode;
import common.plan.node.EqualityFilterNode;
import common.plan.node.PlanNode;
import common.plan.node.ProjectNode;
import common.plan.node.VariableEqualityFilterNode;

/** Stores helper functions that are used during bashlog compilation */
public class AwkHelper {

  /** Escape string for usage in awk */
  public static String escape(String str) {
    return str.replaceAll("\"", "\\\"").replaceAll("'", "'\\''");
  }

  public static String awkProject(ProjectNode p) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < p.getProjection().length; i++) {
      if (i != 0) sb.append(" FS ");
      if (p.getProjection()[i] >= 0) {
        sb.append("$");
        sb.append(p.getProjection()[i] + 1);
      } else {
        p.getConstant(i).ifPresent(cnst -> sb.append("\"" + escape(cnst.toString()) + "\""));
      }
    }
    return sb.toString();
  }

  public static void awkEquality(EqualityFilterNode planNode, StringBuilder init, StringBuilder cond) {
    if (planNode instanceof ConstantEqualityFilterNode) {
      ConstantEqualityFilterNode n = (ConstantEqualityFilterNode) planNode;
      cond.append("$");
      cond.append(n.getField() + 1);
      cond.append(" == \"");
      cond.append(escape(n.getValue().toString()));
      cond.append("\"");
    } else if (planNode instanceof VariableEqualityFilterNode) {
      VariableEqualityFilterNode n = (VariableEqualityFilterNode) planNode;
      cond.append("$");
      cond.append(n.getField1() + 1);
      cond.append(" == $");
      cond.append(n.getField2() + 1);
    }
  }

  public static <T> String joinStr(Stream<T> outCols, String delimiter) {
    return outCols.map(t -> t.toString()).collect(Collectors.joining(delimiter));
  }

  public static <T> String joinStr(Collection<T> outCols, String delimiter) {
    return joinStr(outCols.stream(), delimiter);
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

  /**
   * Build an AWK program that uses hash table look up instead of "==" if conditions
   * @param plans containing all the same leaf (sub-tree)
   * @param output which file the output should be written
   * @param awkProg accumulator for the awk program
   * @return remaining all plan nodes that couldn't be processed that way
   */
  public static List<PlanNode> complexAwkLine(Collection<PlanNode> plans, String output, StringBuilder awkProg) {
    // replace value-filters by associative array lookup
    // first, collect all filters that can be combined
    Map<List<Integer>, Map<List<Integer>, List<List<Comparable<?>>>>> outputColToFilteredColToValues = new HashMap<>();
    List<PlanNode> remaining = plans.stream().filter(pn -> {
    	return true;
      /*Map<Integer, Comparable<?>> filterCols = new TreeMap<>();
      List<Integer> outputCols = new ArrayList<>();
      getCols(pn, outputCols, filterCols);
      if (outputCols == null || filterCols.size() == 0) return true;
  
      outputColToFilteredColToValues.computeIfAbsent( //
          outputCols, //
          k -> new HashMap<>()) //
          .computeIfAbsent(new ArrayList<>(filterCols.keySet()), k -> new ArrayList<>())//
          .add(new ArrayList<Comparable<?>>(filterCols.values()));
  
      return false;*/
    }).collect(Collectors.toList());
  
    if (outputColToFilteredColToValues.size() > 0) {
      // create arrays outCOLS_condCOLS[VAL] = "1";
      // where COLS looks like 0c1c2
  
      awkProg.append("BEGIN { ");
      outputColToFilteredColToValues.forEach((outCols, map) -> {
        map.forEach((filterCols, values) -> {
          values.forEach(vals -> {
            if (output != null) {
              awkProg.append(output.replace("tmp/", ""));
            }
            awkProg.append("out").append(joinStr(outCols, "c"));
            awkProg.append("_cond").append(joinStr(filterCols, "c"));
            awkProg.append("[\"").append(joinStr(vals, "\" FS \"")).append("\"] = \"1\"; ");
          });
        });
      });
      awkProg.append(" } ");
  
      // filter lines using arrays
      outputColToFilteredColToValues.forEach((outCols, map) -> {
        awkProg.append("(");
        awkProg.append(map.keySet().stream().map(filterCols -> {
          String condition = (String) "(" + joinStr(filterCols.stream().map(i -> "$" + (i + 1)), " FS ") + ")" + //
          " in ";
          if (output != null) {
            condition += output.replace("tmp/", "");
          }
          condition += "out" + joinStr(outCols, "c") + "_cond" + joinStr(filterCols, "c");
  
          return condition;
        }).collect(Collectors.joining(" || ")));
  
        awkProg.append(") ");
        awkProg.append("{ print ").append(joinStr(outCols.stream().map(i -> "$" + (i + 1)), " FS "));
        if (output != null) {
          awkProg.append(" >> \"").append(output).append("\"");
        }
        awkProg.append(" } ");
      });
    }
    return remaining;
  }

  /**
   * Translate select/project to awk, and return first plan that cannot be translated this way
   * @param plan consisting of selections and (at most one) projections
   * @param output if null output to stdout, otherwise output to file
   * @return
   */
  public static PlanNode simpleAwkLine(PlanNode plan, String output, StringBuilder arg) {
    ProjectNode p = null;
    List<String> init = new ArrayList<>();
    List<String> conditions = new ArrayList<>();
    do {
      if (plan instanceof ProjectNode) {
        if (p != null) throw new IllegalStateException("currently only one projection supported");
        p = (ProjectNode) plan;
        plan = p.getTable();
      } else if (plan instanceof EqualityFilterNode) {
        StringBuilder initLine = new StringBuilder(), condLine = new StringBuilder();
        awkEquality((EqualityFilterNode) plan, initLine, condLine);
        conditions.add(condLine.toString());
        plan = ((EqualityFilterNode) plan).getTable();
      } else {
        break;
      }
    } while (true);
    if (init.size() > 0) {
      arg.append("BEGIN { ");
      arg.append(init.stream().collect(Collectors.joining(" ")));
      arg.append(" }");
    }
    arg.append(conditions.stream().collect(Collectors.joining(" && ")));
    arg.append(" { print ");
    if (p == null) {
      arg.append("$0");
    } else {
      arg.append(awkProject(p));
    }
    if (output != null) {
      arg.append(" >> \"").append(output).append("\"");
    }
    arg.append("} \n ");
    return plan;
  }

  public final static String AWK = "$awk -v FS=$'\\t' '";

}
