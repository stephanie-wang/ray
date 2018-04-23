import ray
import time
import logging
from collections import namedtuple


Node = namedtuple('Node', ['index', 'parent', 'children'])

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

NUM_TRIALS = 30

@ray.remote
def ancestor(graph_id, node_index, task_delay_ms):
    graph = ray.get(graph_id[0])
    node = graph[node_index]
    time.sleep(float(task_delay_ms) / 1000)

    if node.parent is None:
        return [node.index]

    ancestors = ray.get(ancestor.remote(graph_id, node.parent, task_delay_ms))
    return [node.index] + ancestors

@ray.remote
def ancestors(graph_id, node_indices, task_delay_ms):
    return [ancestor.remote(graph_id, node_index, task_delay_ms) for node_index in node_indices]

@ray.remote
def get_leaves(graph_id, node_index, task_delay_ms):
    graph = ray.get(graph_id[0])
    node = graph[node_index]
    time.sleep(float(task_delay_ms) / 1000)

    if len(node.children) == 0:
        return [node.index]

    children_leaves = ray.get([get_leaves.remote(graph_id, child, task_delay_ms) for child in node.children])
    return [leaf for leaves in children_leaves for leaf in leaves]

def make_tree(depth, degree):
    index = 0

    level = [Node(index, None, [])]
    tree = level
    index += 1
    for i in range(depth):
        next_level = []
        for node in level:
            for j in range(degree):
                child = Node(index, node.index, [])
                next_level.append(child)
                node.children.append(child.index)

                index += 1

        tree += next_level
        level = next_level

    return tree

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--depth', type=int, required=True)
    parser.add_argument('--degree', type=int, required=True)
    parser.add_argument('--task-delay-ms', type=int, default=1)

    args = parser.parse_args()

    tree = make_tree(args.depth, args.degree)
    root = 0

    ray.init(use_raylet=True, redirect_output=False)
    tree_id = ray.put(tree)
    for _ in range(NUM_TRIALS):
        time.sleep(1)

        start = time.time()

        leaves = [get_leaves.remote([tree_id], 0, args.task_delay_ms)]
        path_id_lists = ray.get([ancestors.remote([tree_id], leaf, args.task_delay_ms) for leaf in leaves])
        path_ids = [path_id for path_id_list in path_id_lists for path_id in path_id_list]
        paths = ray.get(path_ids)
        log.info("Ray took %f seconds", time.time() - start)
