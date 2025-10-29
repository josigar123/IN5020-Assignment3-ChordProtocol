package protocol;


import crypto.ConsistentHashing;
import p2p.FingerTableEntry;
import p2p.NetworkInterface;
import p2p.NodeInterface;


import java.util.*;

/**
 * This class implements the chord protocol. The protocol is tested using the custom built simulator.
 */
public class ChordProtocol implements Protocol{

    // length of the identifier that is used for consistent hashing
    public int m;

    // network object
    public NetworkInterface network;

    // consisent hasing object
    public ConsistentHashing ch;

    // key indexes. tuples of (<key name>, <key index>)
    public HashMap<String, Integer> keyIndexes;


    public ChordProtocol(int m){
        this.m = m;
        setHashFunction();
        this.keyIndexes = new HashMap<String, Integer>();
    }



    /**
     * sets the hash function
     */
    public void setHashFunction(){
        this.ch = new ConsistentHashing(this.m);
    }

  

    /**
     * sets the network
     * @param network the network object
     */
    public void setNetwork(NetworkInterface network){
        this.network = network;
    }


    /**
     * sets the key indexes. Those key indexes can be used to  test the lookup operation.
     * @param keyIndexes - indexes of keys
     */
    public void setKeys(HashMap<String, Integer> keyIndexes){
        this.keyIndexes = keyIndexes;
    }



    /**
     *
     * @return the network object
     */
    public NetworkInterface getNetwork(){
        return this.network;
    }






    /**
     * This method builds the overlay network.  It assumes the network object has already been set. It generates indexes
     *     for all the nodes in the network. Based on the indexes it constructs the ring and places nodes on the ring.
     *         algorithm:
     *           1) for each node:
     *           2)     find neighbor based on consistent hash (neighbor should be next to the current node in the ring)
     *           3)     add neighbor to the peer (uses Peer.addNeighbor() method)
     */
    public void buildOverlayNetwork(){

        LinkedHashMap<String, NodeInterface> nodes =  network.getTopology();
        TreeMap<Integer, String> indexMapping = new TreeMap<>();

        for(String name: nodes.keySet()){
            Integer index = ch.hash(name);
            nodes.get(name).setId(index);
            indexMapping.put(index, name);
        }
       
        List<String> sortedNodes = new ArrayList<>(indexMapping.values());
        int n = sortedNodes.size();

        for (int i = 0; i < n; i++) {
            String currentName = sortedNodes.get(i);
            String nextName = sortedNodes.get((i + 1) % n);

            NodeInterface currentNode = nodes.get(currentName);
            NodeInterface nextNode = nodes.get(nextName);

            currentNode.addNeighbor(nextName, nextNode);
        }
    }


    /**
     * This method builds the finger table. The finger table is the routing table used in the chord protocol to perform
     * lookup operations. The finger table stores m-entries. Each ith entry points to the ith finger of the node.
     * Each ith entry stores the information of it's neighbor that is responsible for indexes ((n+2^i-1) mod 2^m).
     * i = 1,...,m.
     *
     *Each finger table entry should consists of
     *     1) start value - (n+2^i-1) mod 2^m. i = 1,...,m
     *     2) interval - [finger[i].start, finger[i+1].start)
     *     3) node - first node in the ring that is responsible for indexes in the interval
     */
    public void buildFingerTable() {

        // Get all nodes in the network
        LinkedHashMap<String, NodeInterface> topology = network.getTopology();

        // For each node, build its finger table
        for(Map.Entry<String, NodeInterface> entry : topology.entrySet()){

            // This nodes finger table
            ArrayList<FingerTableEntry> fingerTable = new  ArrayList<>();
            NodeInterface node = entry.getValue();

            // Build each entry for the table of the current node
            for(int i = 1; i <= m; i++){

                // Get the start for this entry
                int start = (int) ((node.getId() + Math.pow(2, i - 1)) % Math.pow(2, m));

                // End of the interval
                int end = (int) ((node.getId() + Math.pow(2, i)) % Math.pow(2, m));

                // Get the successor on the interval
                NodeInterface successor = findSuccessor(topology.values(), start);

                // Add the entry to the table with the appropriate fields
                fingerTable.add(new FingerTableEntry(start, end, successor));
            }

            // Set the current nodes rounting table
            node.setRoutingTable(fingerTable);
        }
    }

    // Method is needed to find the appropriate predecessor given a start, search the topology
    private NodeInterface findSuccessor(Collection<NodeInterface> topology, int target){

        // crete a list of the nodes in the network, and sort them based on ID
        List<NodeInterface> sortedNodes = new ArrayList<>(topology);
        sortedNodes.sort(Comparator.comparingInt(NodeInterface::getId));

        // Find the node with appropriate id
        for(NodeInterface node : sortedNodes){
            // Want to find the first node with an id greater or equal to target
            // If it doesnt exist we wrap around and return the first of the sorted nodes (see last line of method)
            if(node.getId() >= target){
                return node;
            }
        }

        // Wrap-around case, no nodes with suitable ID found, we have gone through the ring
        return sortedNodes.get(0);
    }

    /**
     * This method performs the lookup operation.
     *  Given the key index, it starts with one of the node in the network and follows through the finger table.
     *  The correct successors would be identified and the request would be checked in their finger tables successively.
     *   Finally the request will reach the node that contains the data item.
     *
     * @param keyIndex index of the key
     * @return names of nodes that have been searched and the final node that contains the key
     */
    public LookUpResponse lookUp(int keyIndex){

        // choose fixed start node (first in topology)
        NodeInterface curr = this.network.getTopology().entrySet().iterator().next().getValue();

        LinkedHashSet<String> route = new LinkedHashSet<>();
        // prevents infinite looping if something goes wrong
        HashSet<String> visited = new HashSet<>();

        while (curr != null && !visited.contains(curr.getName())) {
            // mark this node as visited and add to the lookup route
            visited.add(curr.getName());
            route.add(curr.getName());

        // check if this node already stores the key (then lookup is done)
        Object dataObj = curr.getData();
        if (dataObj instanceof LinkedHashSet<?>) {
            LinkedHashSet<?> data = (LinkedHashSet<?>) dataObj;
            if (data.contains(keyIndex)) {
                return new LookUpResponse(route, curr.getId(), curr.getName());
            }
        }

        // Save ring-successor for fallback use
        NodeInterface successor = curr.getSuccessor();

        // Try to locate correct next-hop using the finger table
        NodeInterface next = null;
        Object rt = curr.getRoutingTable();

        if (rt instanceof List<?>) {
            List<?> list = (List<?>) rt;

                // Traverse fingers from largest interval (end) to smallest (start)
                for (int i = list.size() - 1; i >= 0; i--) {
                    Object o = list.get(i);
                    if (!(o instanceof FingerTableEntry)) continue;
                    FingerTableEntry fe = (FingerTableEntry) o;

                    // Check if keyIndex lies within this finger's [start, end) interval (with wrap)
                    boolean inInterval;
                    if (fe.start < fe.end) {
                        inInterval = (keyIndex >= fe.start && keyIndex < fe.end);
                    } else if (fe.start > fe.end) {
                        inInterval = (keyIndex >= fe.start) || (keyIndex < fe.end);
                    } else {
                        inInterval = true;
                    }

                    // Use the successor of that finger interval as next hop
                    if (inInterval) {
                        next = fe.successor;
                        break;
                    }
                }
            }

            // If finger table gave no match, fall back to the immediate ring successor
            if (next == null) {
                next = successor;
            }

            // If no movement is possible, abort
            if (next == null) break;

            // Move to the next node and continue lookup
            curr = next;
        }

        // Safety fallback: resolve responsible node using ring logic if lookup loop exits unexpectedly
        NodeInterface responsible = findSuccessor(this.network.getTopology().values(), keyIndex);
        if (responsible != null) {
            route.add(responsible.getName());
            return new LookUpResponse(route, responsible.getId(), responsible.getName());
        }

        return null;
    }



}
