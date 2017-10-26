package experiments.lubm.generator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class DatalogWriter implements Writer {

  final static String PATH = "/home/tr/extern/data/bashlog/lubm/";

  String path;

  String currentInstance = null;

  PrintWriter writer;

  public DatalogWriter(String path) throws FileNotFoundException {
    new File(path).mkdirs();
    this.path = path;
    writer = new PrintWriter(path + "all.datalog");
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
    writer.append(Generator.CLASS_TOKEN[classType]).append("(\"").append(id).append("\").").println();
  }

  @Override
  public void startAboutSection(int classType, String id) {
    currentInstance = id;
    writer.append(Generator.CLASS_TOKEN[classType]).append("(\"").append(id).append("\").").println();
  }

  @Override
  public void endSection(int classType) {
    currentInstance = null;
  }

  @Override
  public void addProperty(int property, String value, boolean isResource) {
    writer//
        .append(Generator.PROP_TOKEN[property]).append("(\"").append(currentInstance).append("\", \"").append(value).append("\").").println();
  }

  @Override
  public void addProperty(int property, int valueClass, String valueId) {
    writer//
        .append(Generator.PROP_TOKEN[property]).append("(\"").append(currentInstance).append("\", \"").append(valueId).append("\").").println();
  }

  public static void generate(int univNum, int startIndex, int seed, String path) throws FileNotFoundException {
    new Generator().start(univNum, startIndex, seed, new DatalogWriter(path), "dummy");   
  }

  public static void main(String[] args) throws FileNotFoundException {
    if (args.length == 0) args = new String[] { "1" };
    int univNum = Integer.parseInt(args[0]);
    int startIndex = 0;
    int seed = 0;
    generate(univNum, 0, 0, PATH + "/" + univNum + "/");
  }
}
