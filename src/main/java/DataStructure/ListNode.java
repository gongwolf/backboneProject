package DataStructure;

public class ListNode<T> {
    public T data;
    public ListNode<T> next;
    public ListNode<T> prev;


    public ListNode(T data) {
        this.data = data;
        this.next = null;
        this.prev = null;
    }

    public void setNext(ListNode<T> next) {
        this.next = next;
    }

    public void setPrev(ListNode<T> next) {
        this.prev = next;
    }

    public ListNode<T> getNext() {
        return next;
    }

    public ListNode<T> getPrev() {
        return prev;
    }

    public T getData() {
        return data;
    }

    public void delete(){
        ListNode<T> p = this.prev;
        ListNode<T> n = this.next;
        p.setNext(n);
        n.setPrev(p);
    }
}