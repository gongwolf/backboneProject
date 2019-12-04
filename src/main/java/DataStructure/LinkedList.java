package DataStructure;

public class LinkedList<T> {
    public ListNode<T> head;
    public ListNode<T> tail;
    public int n;

    public LinkedList() {
        head = null;
        tail = null;
        n = 0;
    }

    public void append(T data) {
        ListNode<T> node = new ListNode<>(data);
        if (head == null) {
            head = tail = node;
        } else {
            ListNode<T> last = tail;
            last.setNext(node);
            node.setNext(null);
            node.setPrev(last);
            tail = node;
        }
        n++;
    }


    public void append(ListNode<T> node) {
        if (head == null) {
            head = tail = node;
        } else {
            ListNode<T> last = tail;
            last.setNext(node);
            node.setNext(null);
            node.setPrev(last);
            tail = node;
        }
        n++;
    }

    public boolean isEmpty() {
        return head == null && tail == null;
    }

    public void updateNumber() {
        int counter = 0;
        if (!isEmpty()) {
            ListNode<T> current = head;
            while (current != tail) {
                current = current.next;
                counter++;
            }
            counter++;
        }

        this.n = counter;
    }
}


