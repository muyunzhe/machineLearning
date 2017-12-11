#-*- encoding: utf-8 -*-

class Node():
    def __init__(self, value):
        self.value = value
        self.next = None

    def __repr__(self):
        return "Node<{}>".format(self.value)


def delete_node(p_head, node):
    if not p_head or not node:
        return False

    if node.next:
        node.value = node.next.value
        node.next = node.next.next
    elif p_head == node and p_head.next is None:
        p_head = None
        node = None
        return True
    p_node = p_head
    while p_node.next != node:
        p_node = p_node.next
    p_node.next = None
    return True
    # 给定的结点不属于链表

    return False


def build_link(values):
    if not values:
        return None
    root = Node(values[0])
    p_node = root
    for value in values:
        p_node.next = Node(value)
        p_node = p_node.next

    return root

def main():
    p_head = build_link([1, 2, 3, 4, 5])
    node_3 = p_head.next.next # Node<3>
    delete_node(p_head, node_3)
    print('\n')
    print('delete last node')
    node_5 = p_head.next.next.next
    delete_node(p_head, node_5)
if __name__ == '__main__':
    main()