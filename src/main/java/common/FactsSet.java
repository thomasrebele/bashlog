package common;

import java.util.Set;
import java.util.stream.Stream;

public interface FactsSet {
  Set<String> getRelations();

  Stream<Comparable<?>[]> getByRelation(String relation);
}
