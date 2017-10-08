package common;

import java.util.Set;

import common.parser.Program;
import flinklog.FactsSet;

public interface Evaluator {

  FactsSet evaluate(Program program, FactsSet facts, Set<String> relationsToOutput) throws Exception;

  default void debug(Program program, FactsSet facts, Set<String> relationsToOutput) throws Exception {
    throw new UnsupportedOperationException();
  }
}