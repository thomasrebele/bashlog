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

  public static int count(boolean[] array, boolean val) {
    int c = 0;
    for (int i = 0; i < array.length; i++) {
      if (array[i] == val) c++;
    }
    return c;
  }

  public static int[] inverse(int[] array) {
    return inverse(array, array.length);
  }

  public static int[] inverse(int[] array, int length) {
    int[] r = new int[length];
    for (int i = 0; i < array.length; i++) {
      if (array[i] < length) r[array[i]] = i;
    }
    return r;
  }

  public static int[] apply(int[] a, int[] aToB) {
    int[] b = new int[a.length];
    for (int i = 0; i < a.length; i++) {
      b[i] = aToB[a[i]];
    }
    return b;
  }

  public static boolean isIdentityProjection(int[] projection) {
    for (int i = 0; i < projection.length; i++) {
      if (projection[i] != i) {
        return false;
      }
    }
    return true;
  }

}
