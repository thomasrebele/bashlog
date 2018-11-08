package bashlog;

import org.junit.Rule;
import org.junit.rules.Timeout;

import common.Evaluator;

public class BashlogLUBMTest extends common.LUBMTest {

  @Override
  public Evaluator evaluator() {
    return new BashlogEvaluator("/tmp/bashlog-test/", false);
  }

  @Rule
  public Timeout globalTimeout = Timeout.seconds(20); // limit execution time for a test */

}
