package bashlog;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.Evaluator;
import common.FactsSet;
import common.SimpleFactsSet;
import common.parser.ParserReader;
import common.parser.Program;
import common.parser.Rule;
import common.TSVWriter;

/** Execute bashlog from java */
public class BashlogEvaluator implements Evaluator {

  private static final Logger LOG = LoggerFactory.getLogger(BashlogEvaluator.class);

  String workingDir;

  private boolean debug = false;

  public BashlogEvaluator(String workingDir) {
    new File(workingDir).mkdirs();
    this.workingDir = workingDir;
  }

  public BashlogEvaluator(String workingDir, boolean debug) {
    new File(workingDir).mkdirs();
    this.workingDir = workingDir;
    this.debug = debug;
  }

  public FactsSet evaluate(Program program, FactsSet facts, Set<String> relationsToOutput) throws IOException {
    program = program.copy();
    for (String relation : facts.getRelations()) {
      String path = workingDir + "/" + relation.replace("/", "_");
      TSVWriter writer = new TSVWriter(path);
      facts.getByRelation(relation).forEach(row -> {
        List<String> vals = new ArrayList<>();
        for (Comparable<?> i : row) {
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
    if (debug) {
      System.out.println(program);
    }

    SimpleFactsSet result = new SimpleFactsSet();
    for (String relation : relationsToOutput) {
      Runtime run = Runtime.getRuntime();
      BashlogCompiler bc = BashlogCompiler.prepareQuery(program, relation);
      String query = bc.compile();
      if (debug) {
        System.out.println(query);
        System.out.println(bc.debugInfo());
      }
      LOG.debug("running " + relation);
      long start = System.nanoTime();
      Process proc = run.exec(new String[] { "/bin/bash", "-c", query });
      BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
      String line;
      while ((line = br.readLine()) != null) {
        result.add(relation, line.split("\t"));
      }
      LOG.debug("bash command executed in " + (System.nanoTime() - start) * 1e-9 + "s");
    }

    return result;
  }

  public static void main(String[] args) throws Exception {

    String src = "rule(X) :- rel1(X), rel2(Y), rel3(X,Y).\n cp(X,Y) :- rel1(X), rel2(Y). ";
    src += "rule2(X) :- rel3(X, Y), rel3(Z, W), rel1(X), rel2(W).";
    Program p = Program.read(new ParserReader(src), BashlogCompiler.BASHLOG_PARSER_FEATURES);

    SimpleFactsSet facts = new SimpleFactsSet();
    facts.add("rel1/1", "a");
    facts.add("rel2/1", "b");
    facts.add("rel3/2", "a", "b");
    new BashlogEvaluator("/tmp/bashlog-test/").evaluate(p, facts, new HashSet<>(Arrays.asList("rule/1")));
  }

}
