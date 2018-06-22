package common;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class TSVReader implements Iterable<List<String>>, Iterator<List<String>>, Closeable {

  BufferedReader reader;

  List<String> next = null;

  public TSVReader(File path) throws IOException {
    reader = new BufferedReader(new FileReader(path));
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  @Override
  public Iterator<List<String>> iterator() {
    return this;
  }

  private List<String> peek() {
    if (reader == null) return null;
    if (next == null) {
      String line;
      try {
        line = reader.readLine();
        next = Arrays.asList(line.split("\t"));
      } catch (IOException e) {
        e.printStackTrace();
        reader = null;
      }
    }
    return next;
  }

  @Override
  public boolean hasNext() {
    return peek() != null;
  }

  @Override
  public List<String> next() {
    try {
      return peek();
    } finally {
      next = null;
    }
  }
}
