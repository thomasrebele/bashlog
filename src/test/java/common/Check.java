package common;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import yago4.predicates.FlowControl;
import yago4.predicates.Predicate;
import yago4.reasoner.Worker;
import yago4.reasoner.YVariable;

public class Check implements AutoCloseable {

  public final static Logger log = LoggerFactory.getLogger(Check.class);

  Map<List<Object>, Integer> expected = new HashMap<>(), expectedParts = new HashMap<>();

  Map<List<Object>, Integer> unexpected = new HashMap<>();

  boolean debug = true;

  boolean ignoreUnexpected = false;

  boolean ignoreTooOften = false;

  public void ignoreUnexpected() {
    this.ignoreUnexpected = true;
  }

  public void ignoreTooOften() {
    this.ignoreTooOften = true;
  }

  public boolean apply(Object... vals) {
    return apply(Arrays.stream(vals).collect(Collectors.toList()));
  }

  public boolean apply(List<Object> vals) {
    log.debug("got " + vals);
    Integer count = expected.computeIfPresent(vals, (l, c) -> --c);
    if (count == null) {
      next_exp: for (List<Object> os : expectedParts.keySet()) {
        for (int i = 0; i < os.size(); i++) {
          if (os.get(i) != null) {
            if (!Objects.equals(os.get(i), vals.get(i))) {
              continue next_exp;
            }
          }
        }
        count = expectedParts.computeIfPresent(os, (l, c) -> --c);
      }
    }
    if (count == null) {
      unexpected.merge(vals, 1, (a, b) -> a + b);
    }

    return true;
  }

  @Override
  public void close() throws Exception {
    log.debug("closing worker, unexpected " + unexpected.size());
    boolean fail[] = { false };
    int[] correct = { 0 };

    try {
      int count[] = { 0 };
      BiConsumer<List<Object>, Integer> f = (l, c) -> {
        if (c > 0) {
          log.error("missing (" + c + "x): " + l);
          fail[0] = true;
          if (count[0]++ > 10) return;
        }
        if (c < 0 && !ignoreTooOften) {
          log.error("too often (" + c + "x): " + l);
          fail[0] = true;
          if (count[0]++ > 10) return;
        }
        if (c == 0) {
          correct[0]++;
        }
      };

      expected.forEach(f);
      expectedParts.forEach(f);
      if (!ignoreUnexpected) {
        unexpected.forEach((l, c) -> {
          log.error("unexpected (" + c + "x): " + l);
          fail[0] = true;
          if (count[0]++ > 10) return;
        });
      }
    } finally {
      if (fail[0]) {
        log.info("found at least " + correct[0] + " correct entries");
        Assert.fail("unexpected output, check console output");
      }
    }
  }

  public <T> void onceList(List<T> objects) {
    timesList(1, objects);
  }

  public void once(Object... objects) {
    times(1, objects);
  }

  @SuppressWarnings("unchecked")
  public <T> void timesList(int times, List<T> objects) {
    for (Object o : objects) {
      if (o == null) {
        expectedParts.put((List<Object>) objects, times);
        return;
      }
    }
    expected.put((List<Object>) objects, times);
  }

  public void times(int times, Object... objects) {
    timesList(times, Arrays.asList(objects));
  }

  public static Worker stringCheck(Check c) {
    return new Worker() {
  
      @Override
      public FlowControl apply(List<YVariable> args) {
        List<Object> vals = new ArrayList<Object>(Predicate.stringValues(args));
        return FlowControl.from(c.apply(vals));
      }
  
      @Override
      public void close() throws Exception {
        c.close();
      }
  
    };
  }
}
