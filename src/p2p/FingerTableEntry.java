package p2p;

public class FingerTableEntry {

    int start;
    int end;
    NodeInterface successor;

    public FingerTableEntry(int start, int end, NodeInterface successor) {
        this.start = start;
        this.end = end;
        this.successor = successor;
    }
}
