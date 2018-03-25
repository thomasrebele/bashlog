package common;


import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import common.parser.CompoundTerm;
import common.parser.Constant;
import common.parser.Parseable;
import common.parser.ParserReader;

public class SimpleFactsSet implements FactsSet {

  private Map<String, Set<Comparable<?>[]>> facts = new HashMap<>();

  public void add(String relation, Comparable<?>... args) {
    facts.computeIfAbsent(relation, k -> new HashSet<>()).add(args);
  }

  public void add(CompoundTerm term) {
    Comparable<?>[] args = new Comparable[term.args.length];
    for (int i = 0; i < term.args.length; i++) {
      args[i] = ((Constant<?>) term.args[i]).getValue();
    }
    add(term.getRelation(), args);
  }

  public void loadFile(Path path) throws IOException {
    ParserReader pr = new ParserReader(Tools.getFileContent(path.toFile()));
    while (true) {
      pr.skipComments();
      if (pr.peek() == null) return;
      CompoundTerm value = CompoundTerm.read(pr, Collections.emptyMap(), Parseable.ALL_FEATURES);
      if (value != null && pr.consume(".") != null) {
        add(value);
      }
    }
  }

  @Override
  public Set<String> getRelations() {
    return facts.keySet();
  }

  @Override
  public Stream<Comparable<?>[]> getByRelation(String relation) {
    //TODO: better datastructure
    return facts.getOrDefault(relation, Collections.emptySet()).stream();
  }

  @Override
  public String toString() {
    return facts.entrySet().stream().flatMap(entry ->
            entry.getValue().stream().map(value ->
                    entry.getKey() + "(" + Arrays.stream(value).map(Object::toString).collect(Collectors.joining(", ")) + ")"
            )
    ).collect(Collectors.joining("\n"));
  }
}
