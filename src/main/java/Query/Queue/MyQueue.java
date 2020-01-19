package Query.Queue;


import java.util.LinkedList;
import java.util.Queue;

public class MyQueue {
    Queue<myBackNode> queue;

    public MyQueue() {
        this.queue = new LinkedList<>();
    }

    public boolean add(myBackNode n) {
        n.inqueue = true;
        return this.queue.add(n);
    }

    public int size() {
        return this.queue.size();
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public myBackNode pop() {
        myBackNode n = this.queue.poll();
        n.inqueue = false;
        return n;
    }
}
