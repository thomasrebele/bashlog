package common;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Tools {

  public static int[] sequence(int start, int end) {
    int[] result = new int[end - start];
    for (int i = 0; i < result.length; i++) {
      result[i] = start + i;
    }
    return result;
  }

  public static int[] sequence(int arity) {
    return sequence(0, arity);
  }

  public static int[] concat(int[] first, int[] second) {
    int[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  public static <T> Set<T> set(@SuppressWarnings("unchecked") T... items) {
    return new HashSet<>(Arrays.asList(items));
  }

}
