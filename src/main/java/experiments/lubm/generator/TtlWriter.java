package experiments.lubm.generator;

import java.io.*;

public class TtlWriter implements Writer {

  final static String PATH = "/home/tr/extern/data/bashlog/lubm/";

  String path;

  String currentInstance = null;

  PrintWriter writer;

  public TtlWriter(String path) throws FileNotFoundException {
    new File(path).getAbsoluteFile().getParentFile().mkdirs();
    this.path = path;
    writer = new PrintWriter(path);
    writer.write("@base <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> .\n");
    writer.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.\n");
    writer.write("@prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>.\n");
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
    writer.append("<").append(id).append(">\trdf:type\tub:").append(Generator.CLASS_TOKEN[classType]).append("\t.").println();
  }

  @Override
  public void startAboutSection(int classType, String id) {
    startSection(classType, id);
  }

  @Override
  public void endSection(int classType) {
    currentInstance = null;
  }

  @Override
  public void addProperty(int property, String value, boolean isResource) {
    writer.append("<").append(currentInstance).append(">\tub:") //
        .append(Generator.PROP_TOKEN[property]).append("\t");
    if (isResource) {
      writer.append("<").append(value).append(">\t.").println();
    } else {
      writer.append("\"").append(value).append("\"\t.").println();
    }
  }

  @Override
  public void addProperty(int property, int valueClass, String valueId) {
    writer.append("<").append(currentInstance).append(">\tub:") //
        .append(Generator.PROP_TOKEN[property]).append("\t");
    //writer.append("\"").append(valueId).append("\".").println();
    writer.append("<").append(valueId).append(">.").println();
  }

  public static void generate(int univNum, int startIndex, int seed, String path) throws FileNotFoundException {
    new Generator().start(univNum, startIndex, seed, new TtlWriter(path), "dummy");   
  }

  public static void main(String[] args) throws FileNotFoundException {
    if (args.length == 0) args = new String[] { "1" };
    int univNum = Integer.parseInt(args[0]);
    String path = PATH + "/" + univNum + "/" + "all.ttl";
    if (args.length >= 2) {
      path = args[1];
    }
    generate(univNum, 0, 0, path);
  }
}
