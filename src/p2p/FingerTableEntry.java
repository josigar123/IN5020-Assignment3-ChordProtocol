package p2p;

// Represents an entry in a nodes finger table
public class FingerTableEntry {

    // The start id of an entry
    public int start;
    //  The end id of an entry, can complete the interval by combining (start,end)
    public int end;
    public NodeInterface successor;

    // Constructor for entry
    public FingerTableEntry(int start, int end, NodeInterface successor) {
        this.start = start;
        this.end = end;
        this.successor = successor;
    }

    // An overridden toString for nice printing for results
    @Override
    public String toString() {
        return "[Start: " + start + ", Interval: (" + start + ", " + end + "), Successor: " + successor.getName() + "]";
    }
}
