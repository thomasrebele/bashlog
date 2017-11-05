package bashlog;

import common.Evaluator;
import common.parser.ParserReader;
import common.parser.Program;
import common.parser.Rule;
import flinklog.FactsSet;
import flinklog.SimpleFactsSet;
import javatools.filehandlers.TSVWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class BashlogEvaluator implements Evaluator {

  String workingDir;

  public BashlogEvaluator(String workingDir) {
    new File(workingDir).mkdirs();
    this.workingDir = workingDir;
  }

  @Override
  public FactsSet evaluate(Program program, FactsSet facts, Set<String> relationsToOutput) throws Exception {
    return evaluate(program, facts, relationsToOutput, false);
  }

  public FactsSet evaluate(Program program, FactsSet facts, Set<String> relationsToOutput, boolean debug) throws IOException {

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
      program.addRule(Rule.bashRule(relation, "cat " + path));
    }

    SimpleFactsSet result = new SimpleFactsSet();
    for (String relation : relationsToOutput) {
      Runtime run = Runtime.getRuntime();
      String query = BashlogCompiler.compileQuery(program, relation);
      if (debug) {
        System.out.println(query);
      }
      Process proc = run.exec(new String[] { "/bin/bash", "-c", query });
      BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
      String line;
      while ((line = br.readLine()) != null) {
        result.add(relation, line.split("\t"));
      }
    }

    return result;
  }

  public Evaluator debug() {
    return new Evaluator() {
      
      @Override
      public FactsSet evaluate(Program program, FactsSet facts, Set<String> relationsToOutput) throws Exception {
        return BashlogEvaluator.this.evaluate(program, facts, relationsToOutput, true);
      }
    };
    
  }
  
  @Override
  public void debug(Program program, FactsSet facts, Set<String> relationsToOutput) throws Exception {
    evaluate(program, facts, relationsToOutput, true);
  }

  public static void main(String[] args) throws Exception {
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
    String query = BashlogCompiler.compileQuery(p, "rule/1");
    //System.out.println(query);

    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("rel1/1", "a");
    facts.add("rel2/1", "b");
    facts.add("rel3/2", "a", "b");
    new BashlogEvaluator("/tmp/bashlog-test/").evaluate(p, facts, new HashSet<>(Arrays.asList("rule/1")));
  }

}
