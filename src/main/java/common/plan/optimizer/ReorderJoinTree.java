package common.plan.optimizer;

import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.Tools;
import common.plan.node.*;

public class ReorderJoinTree extends ReorderJoinLinear {

  private static final Logger LOG = LoggerFactory.getLogger(ReorderJoinLinear.class);

  @Override
  public PlanNode apply(PlanNode node) {
    return node.transform((o, n, op) -> {
      if (n instanceof JoinNode && !(PlanNode.parent(op) instanceof JoinNode)) {
        return reorder((JoinNode) n);
      } else {
        return n;
      }
    }, new LinkedList<>());
  }

  @Override
  protected PlanNode reorder(JoinNode n) {
    JoinInfo info = new JoinInfo();
    analyze(n, info);
    return reorderTree(n, info);
  }

  protected PlanNode reorderTree(JoinNode n, JoinInfo info) {
    if (info.leaves.size() < 4) {
      return reorderLinear(n, info);
    }
    
    // edges to weight
    Map<Map.Entry<Integer, Integer>, List<int[]>> edgesToConditions = new HashMap<>();
    info.joinConditions.forEach(cond -> {
      edgesToConditions.computeIfAbsent(new AbstractMap.SimpleEntry<Integer, Integer>(cond[0], cond[2]), k -> new ArrayList<>()).add(cond);
    });
    List<Map.Entry<Integer, Integer>> sortedEdges = edgesToConditions.entrySet().stream()
        .sorted((e1, e2) -> Integer.compare(e1.getValue().size(), e2.getValue().size())).map(e -> e.getKey()).collect(Collectors.toList());
    
    // Kruskal's minimum spanning tree algorithm
    // adapted from https://github.com/SleekPanther/kruskals-algorithm-minimum-spanning-tree-mst/blob/master/Kruskal.java
    DisjointSet nodeSet = new DisjointSet(info.leaves.size());
    ArrayList<Map.Entry<Integer, Integer>> mstEdges = new ArrayList<>();
    for (Map.Entry<Integer, Integer> e : sortedEdges) {
      int node1 = e.getKey(), node2 = e.getValue();
      int set1 = nodeSet.find(node1);
      int set2 = nodeSet.find(node2);

      if (set1 != set2) { // the two nodes are not yet connected by a path
        mstEdges.add(e);
        nodeSet.union(set1, set2);
        if (mstEdges.size() == info.leaves.size() - 1) break;
      }
    }

    // walk from outside to the inside
    // (assume sets are small, has potential for optimization)
    Map<Integer, Integer> directed = new HashMap<>(), newEdges = new HashMap<>();
    // we have to join leaves in that order, layer by layer
    List<List<Integer>> layers = new ArrayList<>();
    do {
      newEdges.clear();
      for (Map.Entry<Integer, Integer> e : mstEdges) {
        walk(e.getKey(), e.getValue(), directed, newEdges);
        walk(e.getValue(), e.getKey(), directed, newEdges);
      }
      List<Integer> layer = new ArrayList<>();
      int prevSize = directed.size();
      newEdges.forEach((k, v) -> {
        if (v >= 0) {
          directed.put(k, v);
          if (!directed.containsKey(v)) layer.add(k);
        }
      });
      if (directed.size() == prevSize) {
        break;
      }
      if (layer.size() > 0) {
        layers.add(layer);
      }
    } while (newEdges.size() > 0);

    // working list, maps from indexes that we still need to process to plans that we created, and their respective leaves (in order of usage)
    Map<Integer, PlanNode> idxToPlan = new HashMap<>();
    Map<Integer, int[]> idxToLeaves = new HashMap<>();
    for (int i = 0; i < info.leaves.size(); i++) {
      idxToPlan.put(i, info.leaves.get(i));
      idxToLeaves.put(i, new int[] { i });
    }

    // visit each layer
    for (List<Integer> layer : layers) {
      // and join with node pointed by the directed edge
      for (Integer idx : layer) {
        int dst = directed.get(idx);
        PlanNode left = idxToPlan.get(idx);
        PlanNode right = idxToPlan.get(dst);
        int[] leavesLeft = idxToLeaves.get(idx), leavesRight = idxToLeaves.get(dst);
        int[] myLeaves = Tools.concat(idxToLeaves.get(idx), idxToLeaves.get(dst));
        Set<Integer> leavesRightSet = Arrays.stream(leavesRight).boxed().collect(Collectors.toSet());
        
        // fetch all conditions that apply between left and right
        List<int[]> conds = new ArrayList<>();
        for (Integer leaf1 : leavesLeft) {
          for (Integer leaf2 : leavesRight) {
            conds.addAll(edgesToConditions.getOrDefault(new AbstractMap.SimpleEntry<Integer, Integer>(leaf1, leaf2), Collections.emptyList()));
            conds.addAll(edgesToConditions.getOrDefault(new AbstractMap.SimpleEntry<Integer, Integer>(leaf2, leaf1), Collections.emptyList()));
          }
        }

        // left and right join projection
        int[] prjLeft = new int[conds.size()];
        int[] prjRight = new int[conds.size()];

        // condition is [leftLeafIdx, leftColIdx, rightLeafIdx, rightColIdx]
        for (int prjIdx = 0; prjIdx < conds.size(); prjIdx++) {
          int[] cond = conds.get(prjIdx);
          if (leavesRightSet.contains(cond[0])) {
            cond = new int[] { cond[2], cond[3], cond[0], cond[1] };
          }

          prjLeft[prjIdx] = leafIdxToJoinIdx(info.leaves, leavesLeft, cond[0], cond[1]);
          prjRight[prjIdx] = leafIdxToJoinIdx(info.leaves, leavesRight, cond[2], cond[3]);
        }

        PlanNode join = left.join(right, prjLeft, prjRight);

        // update
        idxToLeaves.put(dst, myLeaves);
        idxToLeaves.remove(idx);
        idxToPlan.put(dst, join);
        idxToPlan.remove(idx);
      }
    }

    // cross product for remaining plans
    List<PlanNode> remaining = new ArrayList<>();
    int[] newLeavesOrder = new int[] {};
    for (int idx : idxToPlan.keySet()) {
      remaining.add(idxToPlan.get(idx));
      newLeavesOrder = Tools.concat(newLeavesOrder, idxToLeaves.get(idx));
    }
    List<PlanNode> newLeaves = Arrays.stream(newLeavesOrder).mapToObj(info.leaves::get).collect(Collectors.toList());

    PlanNode join = remaining.stream().reduce((l, r) -> l.join(r, new int[] {}, new int[] {}))
        .orElseThrow(() -> new IllegalStateException("nothing to join!"));

    return join.project(getFinalProjection(info, newLeaves, newLeavesOrder));
  }

  /** Translate a leaf index and leaf column to an output column */
  private int leafIdxToJoinIdx(List<PlanNode> newLeaves, int[] leafIndices, int dstLeafIdx, int j) {
    for (int k = 0; k < leafIndices.length; k++) {
      int leafIdx = leafIndices[k];
      if (leafIdx == dstLeafIdx) return j;
      j += newLeaves.get(leafIdx).getArity();
    }
    LOG.error("warning: leaf " + dstLeafIdx + " not found in " + Arrays.toString(leafIndices));
    return j;

  }

  /** Mark start as visited, and add an edge (or remove edge if another edge "start -&gt; xyz" exists) */
  private void walk(Integer start, Integer end, Map<Integer, Integer> directed, Map<Integer, Integer> newEdges) {
    if (directed.containsKey(start) || directed.containsKey(end)) return;
    newEdges.merge(start, end, (a, b) -> -1);
  }

  public static void main(String[] args) {
    PlanNode a = new FactNode("a0", "a1"), b = new FactNode("b0", "b1"), c = new FactNode("c0", "c1"), d = new FactNode("d0", "d1");
    PlanNode join = a.join(d, new int[] {}, new int[] {}).join(c, new int[] { 1, 2 }, new int[] { 1, 0 }).join(b, new int[] { 0, 3 },
        new int[] { 0, 1 });
    join = join.project(new int[] { 0, 1, 6, 7, 4, 5, 2, 3 });

    System.out.println(join.toPrettyString());
    System.out.println("---");
    join = new ReorderJoinTree().apply(join);
    System.out.println(join.toPrettyString());
  }
}

// from https://github.com/SleekPanther/kruskals-algorithm-minimum-spanning-tree-mst/blob/master/Kruskal.java
class DisjointSet {

  /** Two-in-one: negative values indicate height, non-negative values are index of parent */
  private int set[];

  public DisjointSet(int numElements) {
    set = new int[numElements];
    for (int i = 0; i < set.length; i++) {
      set[i] = -1; // initial height
    }
  }

  public void union(int node1, int node2) {
    if (set[node2] < set[node1]) {
      set[node1] = node2;
    } else {
      if (set[node1] == set[node2]) {
        set[node1]--;
      }
      set[node2] = node1;
    }
  }
  
  public int find(int x) {
    if (set[x] < 0) {
      return x;
    }
    int next = x;
    while (set[next] > 0) {
      next = set[next];
    }
    return next;
  }
}
