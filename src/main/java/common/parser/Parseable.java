package common.parser;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public interface Parseable {

  public static final String ATOMS = "atoms";
  
  public static final Set<String> ALL_FEATURES = new HashSet<>(Arrays.asList(ATOMS));
  
  
  
}
