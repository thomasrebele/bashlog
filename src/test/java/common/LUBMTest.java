package common;

import common.parser.Program;
import experiments.lubm.BashlogLUBM;
import experiments.lubm.generator.Tsv3Writer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * Run LUBM queries and compare with official answers.
 * TODO: the official answers for query 8 removed characters at the end of some email addresses. Fix it here.
 */
public abstract class LUBMTest {

  private static final Logger LOG = LoggerFactory.getLogger(LUBMTest.class);

  private String answers = "http://swat.cse.lehigh.edu/projects/lubm/answers.zip";

  private String lubm = "data/lubm/";

  private Program lubmProgram;

  private SimpleFactsSet empty = new SimpleFactsSet();

  private static int count = 0;

  private static Map<String, Long> timing = new HashMap<>();

  public abstract Evaluator evaluator();

  @Before
  public void setup() throws IOException {
    String dir = lubm + "/answers/";
    if (!new File(dir + "/answers_query1.txt").exists()) {
      DownloadTools.download(answers, dir);
      DownloadTools.unzip(dir + "/answers.zip", dir);
      // TODO: fix answers for query 8 (replace ".ed" and ".e" at end of line with ".edu"
    }
    dir = lubm + "/1/";
    if (!new File(dir).exists() || !new File(dir + "all").exists()) {
      Tsv3Writer.generate(1, 0, 0, dir);
    }

    timing.merge("make program", -System.nanoTime(), (a, b) -> a + b);
    lubmProgram = BashlogLUBM.lubmProgram3(dir, lubm);
    timing.merge("make program", +System.nanoTime(), (a, b) -> a + b);

    Files.write(Paths.get("/tmp/lubm-program.txt"), lubmProgram.toString().getBytes());
    LOG.info("wrote lubm program to /tmp/lubm-program.txt");
  }

  @AfterClass
  public static void printTiming() {
    timing.forEach((name, time) -> {
      LOG.info("average {}: {}", name, time / 1e9 / count);
    });
  }

  private Stream<Object[]> getRelation(String relation) throws Exception {
    Evaluator e = evaluator();
    Stream<Object[]> fs = e.evaluate(lubmProgram, empty, //
        Collections.singleton(relation)).getByRelation(relation).map(c -> Arrays.stream(c).toArray(Object[]::new)
    );
    count++;
    e.getTiming().forEach((name, time) -> {
      LOG.info("{}: {}", name, time / 1e9);
      timing.merge(name, time, (a, b) -> a + b);
    });

    return fs;
  }

  
  private void query(int i) throws Exception {
    try (Check check = new Check()) {
      try (TSVReader expected = new TSVReader(new File(lubm + "/answers/answers_query" + i + ".txt"))) {
        expected.next();
        for (List<String> line : expected) {
          check.onceList(line);
        }
      }
      check.ignoreTooOften();

      String prefix = "query" + i + "/";
      for (int j = 0; j < 5; j++) {
        String relation = prefix + j;
        if (lubmProgram.rulesForRelation(relation).isEmpty()) continue;
        try {
          getRelation(relation).forEach(check::apply);
          check.close();
          return;
        } catch (AssertionError e) {
          evaluator().debug(lubmProgram, empty, Collections.singleton(relation));
          throw e;
        }
      }
      throw new IllegalStateException("no relation found for query " + i);
    }
  }

  @Test
  public void query1() throws Exception {
    query(1);
  }

  @Test
  public void query2() throws Exception {
    query(2);
  }

  @Test
  public void query3() throws Exception {
    query(3);
  }

  @Test
  public void query4() throws Exception {
    query(4);
  }

  @Test
  public void query5() throws Exception {
    query(5);
  }

  @Test
  public void query6() throws Exception {
    query(6);
  }

  @Test
  public void query7() throws Exception {
    query(7);
  }

  @Test
  public void query8() throws Exception {
    query(8);
  }

  @Test
  public void query9() throws Exception {
    query(9);
  }

  @Test
  public void query10() throws Exception {
    query(10);
  }

  @Test
  public void query11() throws Exception {
    query(11);
  }

  @Test
  public void query12() throws Exception {
    query(12);
  }

  @Test
  public void query13() throws Exception {
    query(13);
  }

  @Test
  public void query14() throws Exception {
    query(14);
  }

}
