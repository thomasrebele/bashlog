package common;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.util.Enumeration;

import common.plan.node.PlanNode;

public class VerifyNodeImplementation {


  public static void main(String[] args) throws Exception {
    Enumeration<URL> roots = VerifyNodeImplementation.class.getClassLoader().getResources("");
    while (roots.hasMoreElements()) {
      URL rootURL = roots.nextElement();
      File root = new File(rootURL.getPath());
      Files.walk(root.toPath()).forEach(path -> {
        File file = path.toFile();
        if (file.getName().endsWith(".class") && !file.getName().contains("$")) {
          String fullClassName = file.getAbsolutePath().replace(root.getAbsolutePath() + "/", "").replace("/", ".");
          fullClassName = fullClassName.substring(0, fullClassName.lastIndexOf("."));

          try {
            Class<?> cls = Class.forName(fullClassName);
            if (!PlanNode.class.isAssignableFrom(cls)) return;

            checkMethod(fullClassName, "equals", Object.class);
            checkMethod(fullClassName, "hashCode");

          } catch (ClassNotFoundException e) {
            System.err.println("Cannot check methods of " + fullClassName);
          }

        }
      });
    }
    System.out.println("verification completed");
  }

  private static void checkMethod(String fullClassName, String method, Class<?>... params) throws ClassNotFoundException {
    Class<?> cls = Class.forName(fullClassName);
    if (cls.isInterface()) return;

    try {
      cls.getDeclaredMethod(method, params);
    } catch (NoSuchMethodException e) {
      System.err.println("Please check whether " + fullClassName + " needs method '" + method + "'");

    } catch (SecurityException e) {
      System.err.println("Cannot check methods of " + fullClassName);
    }
  }
}
