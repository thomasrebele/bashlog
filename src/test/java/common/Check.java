package common;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.junit.Assert;

public class Check implements AutoCloseable {

  Map<List<Object>, Integer> expected = new HashMap<>(), expectedParts = new HashMap<>();

  Map<List<Object>, Integer> unexpected = new HashMap<>();

  boolean ignoreUnexpected = true;

  public void ignoreUnexpected() {
    this.ignoreUnexpected = true;
  }

  public boolean apply(Object... vals) {
    return apply(Arrays.stream(vals).collect(Collectors.toList()));
  }

  public boolean apply(List<Object> vals) {
    System.out.println("got " + vals);
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
    boolean fail[] = { false };

    BiConsumer<List<Object>, Integer> f = (l, c) -> {
      if (c > 0) {
        System.out.println("missing (" + c + "x): " + l);
        fail[0] = true;
      }
      if (c < 0) {
        System.out.println("too often (" + c + "x): " + l);
        fail[0] = true;
      }
    };

    expected.forEach(f);
    expectedParts.forEach(f);
    if (!ignoreUnexpected) {
      unexpected.forEach((l, c) -> {
        System.out.println("unexpected (" + c + "x): " + l);
        fail[0] = true;
      });
    }
    if (fail[0]) {
      Assert.fail("unexpected output, check console output");
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
}
