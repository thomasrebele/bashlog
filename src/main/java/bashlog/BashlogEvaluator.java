package bashlog;

import common.Evaluator;
import common.FactsSet;
import common.SimpleFactsSet;
import common.TSVWriter;
import common.parser.ParserReader;
import common.parser.Program;
import common.parser.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/** Execute bashlog from java */
public class BashlogEvaluator implements Evaluator {

  private static final Logger LOG = LoggerFactory.getLogger(BashlogEvaluator.class);

  /** Directory for storing the facts that are given via the FactsSet parameter during evaluation */
  private final String dataDir;
  
  /** Working directory for bash script */
  private final String workingDir;

  private boolean debug = false;

  private long timeCompile = 0, timeBash = 0; // in nano seconds

  public BashlogEvaluator(String workingDir, String dataDir) {
    new File(workingDir).mkdirs();
    this.workingDir = workingDir;
    this.dataDir = dataDir;
  }

  public BashlogEvaluator(String workingDir, String dataDir, boolean debug) {
    new File(workingDir).mkdirs();
    new File(dataDir).mkdirs();
    this.workingDir = workingDir;
    this.debug = debug;
    this.dataDir = dataDir;
  }

  @Override
  public Map<String, Long> getTiming() {
    HashMap<String, Long> map = new HashMap<>();
    map.put("compile", timeCompile);
    map.put("bash", timeBash);
    return map;
  }

  public FactsSet evaluate(Program program, FactsSet facts, Set<String> relationsToOutput) throws IOException {
    timeCompile = 0;
    timeBash = 0;
    program = program.copy();
    for (String relation : facts.getRelations()) {
      String path = dataDir + "/" + relation.replace("/", "_");
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

    SimpleFactsSet result = new SimpleFactsSet();
    for (String relation : relationsToOutput) {
      Runtime run = Runtime.getRuntime();
      timeCompile -= System.nanoTime();
      BashlogCompiler bc = BashlogCompiler.prepareQuery(program, relation);
      if (debug) {
        bc.enableDebug();
      }
      String query = null;
      try {
        query = bc.compile();
      } finally {
        if (debug) {
          System.out.println(query);
          System.out.println(bc.debugInfo());
        }
      }
      timeCompile += System.nanoTime();
      LOG.debug("running " + relation);
      if (debug) {
        LOG.info("saving program to /tmp/bashlog.sh, and debug info to /tmp/bashlog-debug.txt");
        Files.write(Paths.get("/tmp/bashlog-debug.txt"), bc.debugInfo().getBytes());
        Files.write(Paths.get("/tmp/bashlog.sh"), query.getBytes());
      }
      timeBash -= System.nanoTime();
      long start = System.nanoTime();
      Path progFile = Files.createTempFile("bashlog-eval-", "");
      Files.write(progFile, query.getBytes());
      LOG.info("saving program to {}", progFile);
      Process proc = run.exec(new String[] { "/bin/bash", progFile.toAbsolutePath().toString() }, null, Paths.get(workingDir).toFile());
      BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
      String line;
      while ((line = br.readLine()) != null) {
        result.add(relation, line.split("\t"));
      }
      timeBash += System.nanoTime();
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
    new BashlogEvaluator("/tmp/bashlog-test/", "/tmp/bashlog-test/").evaluate(p, facts, new HashSet<>(Arrays.asList("rule/1")));
  }

}
