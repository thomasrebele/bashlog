package bashlog;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import common.Check;
import common.FactsSet;
import common.SimpleFactsSet;
import common.TSVReader;
import common.parser.Program;

@RunWith(Parameterized.class)
public class FileTest {

  @Parameters(name = "{0}")
  public static Collection<Object[]> getFiles() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    int i = 0;
    for (File directory : new File("src/test/resources").listFiles()) {
      Object[] arr = new Object[] { directory, i++ };
      params.add(arr);
    }
    return params;
  }

  private File directory;

  private Integer index;

  public FileTest(File directory, Integer index) {
    this.directory = directory;
    this.index = index;
  }

  @Test
  public void evaluateProgramInTestDir() throws Exception {
    // load test file

    Path programFile = directory.toPath().resolve("main.txt");
    Set<String> features = BashlogCompiler.BASHLOG_PARSER_FEATURES;
    Program p = Program.loadFile(programFile.toString(), features);

    String queryPred = "main";
    queryPred = p.searchRelation(queryPred);
    if (queryPred == null || queryPred.trim().isEmpty()) {
      queryPred = p.rules().get(p.rules().size() - 1).head.getRelation();
    }

    SimpleFactsSet facts = new SimpleFactsSet();
    FactsSet evaluate = new BashlogEvaluator(directory.toString(), "/tmp/bashlog-tests/").evaluate(p, facts, new HashSet<>(Arrays.asList(queryPred)));

    try (Check check = new Check()) {
      File expectedFile = directory.toPath().resolve("expected.txt").toFile();
      try (TSVReader expected = new TSVReader(expectedFile)) {
        for (List<String> line : expected) {
          check.onceList(line);
        }
      }

      evaluate.getByRelation(queryPred).forEach(check::apply);
    }
  }

}
