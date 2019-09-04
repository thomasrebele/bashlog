package experiments.lubm.generator;

import java.io.*;

/** Write LUBM triples for RDFSlice */
public class NtlitWriter implements Writer {

  final static String PATH = "/home/tr/extern/data/bashlog/lubm/";

  String path;

  String currentInstance = null;

  PrintWriter writer;

  public NtlitWriter(String path) throws FileNotFoundException {
    new File(path).getAbsoluteFile().getParentFile().mkdirs();
    this.path = path;
    writer = new PrintWriter(path);
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
    writer.append("<").append(id).append("> rdf:type ub:").append(Generator.CLASS_TOKEN[classType]).append(" .").println();
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
    writer.append("<").append(currentInstance).append("> ub:") //
        .append(Generator.PROP_TOKEN[property]).append(" ");
    if (isResource) {
      writer.append("<").append(value).append("> .").println();
    } else {
      writer.append("lit:").append(value).append(" .").println();
    }
  }

  @Override
  public void addProperty(int property, int valueClass, String valueId) {
    writer.append("<").append(currentInstance).append("> ub:") //
        .append(Generator.PROP_TOKEN[property]).append(" ");
    writer.append("<").append(valueId).append("> .").println();
  }

  public static void generate(int univNum, int startIndex, int seed, String path) throws FileNotFoundException {
    new Generator().start(univNum, startIndex, seed, new NtlitWriter(path), "dummy");   
  }

  public static void main(String[] args) throws FileNotFoundException {
    if (args.length == 0) args = new String[] { "1" };
    int univNum = Integer.parseInt(args[0]);
    String path = PATH + "/" + univNum + "/" + "all.ntlit";
    if (args.length >= 2) {
      path = args[1];
    }
    generate(univNum, 0, 0, path);
  }
}
