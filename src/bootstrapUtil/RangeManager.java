package bootstrapUtil;

import common.Range;

import java.util.TreeSet;

public class RangeManager {
    private final short MAXRANGE = 1023;
    private TreeSet<Integer> nodeIds;

    public RangeManager() {
        this.nodeIds = new TreeSet<Integer>();
        nodeIds.add(0); // Add the bootstrap node
    }

    public synchronized Response addNode(int nodeId) {
        if (nodeId < 0 || nodeId > MAXRANGE) {
            return new Response(-1, "Invalid Node ID. Must be between 0 and " + MAXRANGE);
        }

        if (nodeIds.contains(nodeId)) {
            return new Response(-1, "Node " + nodeId + " already exists.");
        }

        nodeIds.add(nodeId);
        printRanges();
        return new Response(0, "Node " + nodeId + " added.");
    }

    public synchronized Response removeNode(int nodeId) {
        if (nodeId < 0 || nodeId > MAXRANGE) {
            return new Response(-1, "Invalid node ID. Must be between 0 and " + MAXRANGE);
        }

        if (nodeId == 0) {
            return new Response(-1, "Bootstrap node cannot be removed.");
        }

        if (nodeIds.contains(nodeId)) {
            nodeIds.remove(nodeId);
            printRanges();
            return new Response(0, "Node " + nodeId + " removed.");
        }

        return new Response(-1, "Node " + nodeId + " does not exist.");
    }

    public synchronized Range getRangeForNode(int nodeId) {
        if (nodeId > MAXRANGE) return null;
        if (!nodeIds.contains(nodeId)) return null;
        if (nodeId == 0) {
            if (nodeIds.size() == 1) {
                return new Range(0, MAXRANGE);
            }
            int last = nodeIds.last();
            if (last == 0) return new Range(0, MAXRANGE);

            // Assume node 1023 joins. then last would be 1023 + 1 = 1024 and MAXRANGE + 1 = 1024
            // Therefore 1024 % 1024 is = 0 so node 0's range is 0-0 (only key 0)
            int start = (last + 1) % (MAXRANGE + 1);
            return new Range(start, 0);
        } else {
            // For any non-zero node their range is the previousValue (lower) + 1 to the nodeId
            Integer lower = nodeIds.lower(nodeId);
            if (lower == null) lower = 0;     // This will not execute as bootstrap is always in set but doesn't hurt to add
            int start = lower + 1;
            return new Range(start, nodeId);
        }
    }

    public synchronized void printRanges() {
        System.out.println("Current Node Ranges:");
        for (Integer id : nodeIds) {
            Range range = getRangeForNode(id);
            System.out.println("Node " + id + " range: " + range.getStart() + " - " + range.getEnd());
        }
    }
}
