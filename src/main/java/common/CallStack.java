package common;

import java.util.Stack;

/** 
Copyright 2016 Fabian M. Suchanek

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 


This class represents the current position of a program, i.e. the stack of methods that
have been called together with the line numbers.<BR>
Example:<BR>
<PRE>
   public class Blah {
     public m2() { 
       System.out.println(new CallStack());   // (1)
       System.out.println(CallStack.here());  // (2)
     }
  
     public m1() { 
       m2(); 
     }
  
     public static void main(String[] args) { 
       m1(); 
     }
   }
   --&gt;
      Blah.main(12)-&gt;Blah.m1(8)-&gt;Blah.m2(2)  // Call Stack at (1)
      Blah.m2(3)                             // Method and line number at (2)
</PRE>
*/
public class CallStack {

  /** Holds the call stack */
  private Stack<StackTraceElement> callstack = new Stack<>();

  /** Constructs a call stack from the current program position (without the constructor call)*/
  public CallStack() {
    try {
      throw new Exception();
    } catch (Exception e) {
      StackTraceElement[] s = e.getStackTrace();
      for (int i = s.length - 1; i != 0; i--)
        callstack.push(s[i]);
    }
  }

  /** Returns TRUE if the two call stacks have the same elements*/
  @Override
  public boolean equals(Object o) {
    return (o instanceof CallStack && ((CallStack) o).callstack.equals(callstack));
  }

  /** Returns a nice String for a Stacktraceelement*/
  public static String toString(StackTraceElement e) {
    String cln = e.getClassName();
    if (cln.lastIndexOf('.') != -1) cln = cln.substring(cln.lastIndexOf('.') + 1);
    return (cln + "." + e.getMethodName() + '(' + e.getLineNumber() + ')');
  }

  /** Returns "method(line)-&gt;method(line)-&gt;..." */
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < callstack.size() - 1; i++) {
      s.append(toString(callstack.get(i))).append("->");
    }
    s.append(toString(callstack.get(callstack.size() - 1)));
    return (s.toString());
  }

  /** Gives the calling position as a StackTraceElement */
  public StackTraceElement top() {
    return (callstack.peek());
  }

  /** Gives the calling position */
  public static StackTraceElement here() {
    CallStack p = new CallStack();
    p.callstack.pop();
    return (p.callstack.peek());
  }

  /** Returns the callstack */
  public Stack<StackTraceElement> getCallstack() {
    return callstack;
  }

  /** Pops the top level of this callstack, returns this callstack */
  public CallStack ret() {
    callstack.pop();
    return (this);
  }

  /** Test routine */
  public static void main(String[] args) {
    System.out.println(new CallStack());
    System.out.println(here());
  }

}

