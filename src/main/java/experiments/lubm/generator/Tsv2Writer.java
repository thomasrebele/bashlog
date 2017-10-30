package experiments.lubm.generator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

@Deprecated
public class Tsv2Writer implements Writer {

  final static String PATH = "/home/tr/extern/data/bashlog/lubm/";

  String path;

  String currentInstance = null;

  Map<String, PrintWriter> fileToWriter = new HashMap<>();

  public Tsv2Writer(String path) {
    new File(path).mkdirs();
    this.path = path;
  }

  @Override
  public void start() {
    new File(path).mkdirs();
  }

  @Override
  public void end() {
    fileToWriter.forEach((f, pw) -> pw.close());
  }

  @Override
  public void startFile(String fileName) {
  }

  @Override
  public void endFile() {
  }

  public PrintWriter writer(String filename) {
    return fileToWriter.computeIfAbsent(filename, k -> {
      try {
        return new PrintWriter(path + k);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
      return null;
    });
  }

  @Override
  public void startSection(int classType, String id) {
    currentInstance = id;
    writer(Generator.CLASS_TOKEN[classType]).println(id);
  }

  @Override
  public void startAboutSection(int classType, String id) {
    currentInstance = id;
    writer(Generator.CLASS_TOKEN[classType]).println(id);
  }

  @Override
  public void endSection(int classType) {
    currentInstance = null;
  }

  @Override
  public void addProperty(int property, String value, boolean isResource) {
    writer(Generator.PROP_TOKEN[property])//
        .append(currentInstance).append("\t").append(value).println();
  }

  @Override
  public void addProperty(int property, int valueClass, String valueId) {
    writer(Generator.PROP_TOKEN[property])//
        .append(currentInstance).append("\t").append(valueId).println();
  }

  public static void generate(int univNum, int startIndex, int seed, String path) {
    new Generator().start(univNum, startIndex, seed, new Tsv2Writer(path), "dummy");   
  }

  public static void main(String[] args) {
    int univNum = 1;
    int startIndex = 0;
    int seed = 0;
    generate(1, 0, 0, PATH + "/" + univNum + "/");
  }
}
