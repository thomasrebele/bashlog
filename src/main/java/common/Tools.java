package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;

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

  public static <T> T[] concat(T[] first, T[] second) {
    T[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  public static <T> Set<T> set(@SuppressWarnings("unchecked") T... items) {
    Set<T> set = new HashSet<>(items.length);
    Collections.addAll(set, items);
    return set;
  }

  public static int count(boolean[] array, boolean val) {
    int c = 0;
    for (boolean element : array) {
      if (element == val) c++;
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

  public static boolean isIdentity(int[] projection, int inputSize) {
    if (inputSize != projection.length) return false;
    for (int i = 0; i < projection.length; i++) {
      if (projection[i] != i) {
        return false;
      }
    }
    return true;
  }

  public static OptionalInt findKey(int[] projection, int value) {
    for (int i = 0; i < projection.length; i++) {
      if (projection[i] == value) {
        return OptionalInt.of(i);
      }
    }
    return OptionalInt.empty();
  }

  public static <T> boolean isNullArray(T[] array) {
    for (T val : array) {
      if (val != null) {
        return false;
      }
    }
    return true;
  }

  @FunctionalInterface
  public interface QuadriFunction<T, U, V, W, R> {
    R apply(T t, U u, V v, W w);
  }

  public static <K,V> Map<K,V> with(Map<K,V> map, K key, V value) {
    Map<K,V> newMap = new HashMap<>(map);
    newMap.put(key, value);
    return newMap;
  }

  /** Get constant for column i */
  public static<T> Optional<T> get(T[] array, int i) {
    if (array.length > i) {
      return Optional.ofNullable(array[i]);
    } else {
      return Optional.empty();
    }
  }

  public static int[] addToElements(int[] projection, int shift) {
    return Arrays.stream(projection).map(i -> i + shift).toArray();
  }

  /**
   * Returns the content of the (UTF-8 encoded) file as string. Linebreaks
   * are encoded as unix newlines (\n)
   *
   * @param file  File to get String content from
   * @return      String content of file.
   * @throws IOException
   */
  public static String getFileContent(File file) throws IOException {
    StringBuilder sb = new StringBuilder();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8")));
    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
      sb.append(line);
      sb.append('\n');
    }
    reader.close();
    return sb.toString();
  }

}
