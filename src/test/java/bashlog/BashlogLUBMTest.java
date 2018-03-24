package bashlog;

import org.junit.Rule;
import org.junit.rules.Timeout;

import common.Evaluator;

public class BashlogLUBMTest extends common.LUBMTest {

  @Override
  public Evaluator evaluator() {
    return new BashlogEvaluator("/tmp/bashlog-test/", true);
  }

  @Rule
  public Timeout globalTimeout = Timeout.seconds(20); // limit execution time for a test */

  /*@Override
  public void query2() throws Exception {
    throw new UnsupportedOperationException();
  }*/
  
  /*@Override
  public void query4() throws Exception {
    throw new UnsupportedOperationException();
  }*/

}
