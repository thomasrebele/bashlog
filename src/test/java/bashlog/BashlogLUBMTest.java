package bashlog;

import common.Evaluator;

public class BashlogLUBMTest extends common.LUBMTest {

  @Override
  public Evaluator evaluator() {
    return new BashlogEvaluator("/tmp/bashlog-test/");
  }

}
