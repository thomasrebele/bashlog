package common;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import common.parser.Program;
import experiments.lubm.BashlogLUBM;
import experiments.lubm.generator.Tsv3Writer;
import javatools.filehandlers.TSVFile;
import yago4.Tools;

/**
 * Run LUBM queries and compare with official answers.
 * TODO: the official answers for query 8 removed characters at the end of some email addresses. Fix it here.
 */
public abstract class LUBMTest {

  String answers = "http://swat.cse.lehigh.edu/projects/lubm/answers.zip";

  String lubm = "data/lubm/";

  Program lubmProgram;

  SimpleFactsSet empty = new SimpleFactsSet();

  public abstract Evaluator evaluator();

  public LUBMTest() {

  }

  public LUBMTest(String lubmPath) {
    this.lubm = lubmPath;
  }

  @Before
  public void setup() throws IOException {
    String dir = lubm + "/answers/";
    if (!new File(dir + "/answers_query1.txt").exists()) {
      Tools.download(answers, dir);
      Tools.unzip(dir + "/answers.zip", dir);
      // TODO: fix answers for query 8 (replace ".ed" and ".e" at end of line with ".edu"
    }
    dir = lubm + "/1/";
    if (!new File(dir).exists() || !new File(dir + "all").exists()) {
      Tsv3Writer.generate(1, 0, 0, dir);
    }

    lubmProgram = BashlogLUBM.lubmProgram3(dir, lubm);

    System.out.println(lubmProgram.toString());
  }

  public Stream<Object[]> getRelation(String relation) throws Exception {
    return evaluator().evaluate(lubmProgram, empty, //
        Collections.singleton(relation)).getByRelation(relation).map(c -> {
          Object[] a = Arrays.stream(c).toArray(Object[]::new);
          return a;
        });
  }

  public void query(int i) throws Exception {
    try (Check check = new Check()) {
      try (TSVFile expected = new TSVFile(new File(lubm + "/answers/answers_query" + i + ".txt"))) {
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
          getRelation(relation).forEach(c -> check.apply(c));
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
