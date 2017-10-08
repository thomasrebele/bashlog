package bashlog;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.IntStream;

import common.Evaluator;
import common.parser.*;
import common.plan.LogicalPlanBuilder;
import common.plan.PlanNode;
import flinklog.FactsSet;
import flinklog.SimpleFactsSet;
import javatools.filehandlers.TSVWriter;

public class BashlogEvaluator implements Evaluator {

  String workingDir;

  public BashlogEvaluator(String workingDir) {
    new File(workingDir).mkdirs();
    this.workingDir = workingDir;
  }

  public static String compileQuery(Program p, String query) throws IOException {

    Set<String> builtin = new HashSet<>();
    builtin.add("bash_command");
    Map<String, PlanNode> plan = new LogicalPlanBuilder(builtin, new HashSet<>()).getPlanForProgram(p);

    PlanNode pn = plan.get(query);
    BashlogCompiler bc = new BashlogCompiler(pn);
    try {
      String bash = bc.compile();
      return bash + "\n\n" + bc.debugInfo();
    } catch (Exception e) {
      System.out.println(bc.debugInfo());
      throw (e);
    }
  }

  @Override
  public FactsSet evaluate(Program program, FactsSet facts, Set<String> relationsToOutput) throws IOException {

    program = program.copy();
    for (String relation : facts.getRelations()) {
      String path = workingDir + "/" + relation.replace("/", "_");
      TSVWriter writer = new TSVWriter(path);
      facts.getByRelation(relation).forEach(row -> {
        List<String> vals = new ArrayList<>();
        for (Comparable i : row) {
          vals.add(Objects.toString(i));
        }
        try {
          writer.write(vals);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      writer.close();
      // construct bash command 'cat $path'
      String[] rel = relation.split("/");
      int arity = Integer.parseInt(rel[1]);
      Term[] args = IntStream.range(0, arity).mapToObj(tmpI -> new Variable("tmp_" + tmpI)).toArray(Term[]::new);
      CompoundTerm ct = new CompoundTerm("bash_command");
      Constant<String> c = new Constant<>("cat " + path);
      ct.args = new Term[] { c, new TermList(args) };
      Rule r = new Rule(new CompoundTerm(rel[0], args), ct);
      program.addRule(r);
    }

    SimpleFactsSet result = new SimpleFactsSet();
    for (String relation : relationsToOutput) {
      Runtime run = Runtime.getRuntime();
      String query = compileQuery(program, relation);
      Process proc = run.exec(new String[] { "/bin/bash", "-c", query });
      BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
      String line;
      while ((line = br.readLine()) != null) {
        result.add(relation, line.split("\t"));
      }
    }

    return result;
  }

  public static void main(String[] args) throws IOException {
    /*    args = new String[] { "data/bashlog/recursion/datalog.txt", "tc/2" };
    //args = new String[] { "data/bashlog/fast-join/datalog.txt", "main/3" };
    //args = new String[] { "data/bashlog/fast-join/datalog.txt", "test/3" };
    //args = new String[] { "data/bashlog/wikidata/people.txt", "instance/2" };
    //args = new String[] { "data/bashlog/wikidata/people.txt", "subclassTC/2" };
    args = new String[] { "data/bashlog/wikidata/people.txt", "humanClasses/1" };
    args = new String[] { "data/bashlog/wikidata/people.txt", "people/1" };
    args = new String[] { "data/bashlog/edbt2017/yago/yago-bashlog.txt", "people/1" };
    
    String lubm = lubmScript("/home/tr/extern/data/bashlog/lubm/1/");
    System.out.println(lubm);
    String scriptDir = "data/bashlog/edbt2017/lubm/bashlog/";
    new File(scriptDir).mkdirs();
    String[] files = { "data/lubm/tbox.txt", "data/lubm/queries.txt" };
    for (int i = 1; i <= 14; i++) {
      Program p = program(files);
      p.addRules(Program.read(new ParserReader(lubm)));
      String relation = "query" + i + "/1";
      String script = compileQuery(p, relation);
      Files.write(Paths.get(scriptDir + "query" + i + ".sh"), script.getBytes());
    }*/

    String src = "rule(X) :- rel1(X), rel2(Y), rel3(X,Y).\n cp(X,Y) :- rel1(X), rel2(Y). ";
    src += "rule2(X) :- rel3(X, Y), rel3(Z, W), rel1(X), rel2(W).";
    Program p = Program.read(new ParserReader(src));
    String query = compileQuery(p, "rule/1");
    //System.out.println(query);

    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("rel1/1", "a");
    facts.add("rel2/1", "b");
    facts.add("rel3/2", "a", "b");
    new BashlogEvaluator("/tmp/bashlog-test/").evaluate(p, facts, new HashSet<>(Arrays.asList("rule/1")));
  }

}