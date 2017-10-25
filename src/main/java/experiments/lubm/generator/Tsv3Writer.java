package experiments.lubm.generator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class Tsv3Writer implements Writer {

  final static String PATH = "/home/tr/extern/data/bashlog/lubm/";

  String path;

  String currentInstance = null;

  PrintWriter writer;

  public Tsv3Writer(String path) throws FileNotFoundException {
    new File(path).mkdirs();
    this.path = path;
    writer = new PrintWriter(path + "all.tsv");
  }

  @Override
  public void start() {
    new File(path).mkdirs();
  }

  @Override
  public void end() {
    writer.close();
  }

  @Override
  public void startFile(String fileName) {
  }

  @Override
  public void endFile() {
  }


  @Override
  public void startSection(int classType, String id) {
    currentInstance = id;
    writer.append(id).append("\trdf:type\t").append(Generator.CLASS_TOKEN[classType]).println();
  }

  @Override
  public void startAboutSection(int classType, String id) {
    currentInstance = id;
    writer.append(id).append("\trdf:type\t").append(Generator.CLASS_TOKEN[classType]).println();
  }

  @Override
  public void endSection(int classType) {
    currentInstance = null;
  }

  @Override
  public void addProperty(int property, String value, boolean isResource) {
    writer//
        .append(currentInstance).append("\t").append(Generator.PROP_TOKEN[property]).append("\t").append(value).println();
  }

  @Override
  public void addProperty(int property, int valueClass, String valueId) {
    writer//
        .append(currentInstance).append("\t").append(Generator.PROP_TOKEN[property]).append("\t").append(valueId).println();
  }

  public static void generate(int univNum, int startIndex, int seed, String path) throws FileNotFoundException {
    new Generator().start(univNum, startIndex, seed, new Tsv3Writer(path), "dummy");   
  }

  public static void main(String[] args) throws FileNotFoundException {
    int univNum = 1;
    int startIndex = 0;
    int seed = 0;
    generate(1, 0, 0, PATH + "/" + univNum + "/");
  }
}
