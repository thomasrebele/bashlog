package bashlog;

public class BashlogIntegrationTests extends common.IntegrationTests {

  public BashlogIntegrationTests() {
    super(new BashlogEvaluator("/tmp/bashlog-tests/", true));
  }
}
