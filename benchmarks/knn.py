import heapq
import logging
import numpy as np
import struct
import time

import ray


DEBUG = True

logging.basicConfig()
log = logging.getLogger()
if DEBUG:
    log.setLevel(logging.DEBUG)

LEAF_SIZE = 2 ** 7
BISECT_S = 5
ALPHA = int(1 / 0.3)


# Methods to load MNIST data.
def read_idx(filename):
    with open(filename, 'rb') as f:
        zero, data_type, dims = struct.unpack('>HBB', f.read(4))
        shape = tuple(struct.unpack('>I', f.read(4))[0] for d in range(dims))
        return np.fromstring(f.read(), dtype=np.uint8).reshape(shape)


def load_mnist(mnist_filename):
    data = read_idx(mnist_filename)
    data = np.reshape(data, (data.shape[0], data.shape[1] * data.shape[2])).T
    return data


# Helper methods for matrix operations.
def center(X):
    mean = X.mean(axis=1)
    return X - mean[:, np.newaxis]


def bisect(X):
    q = np.zeros(X.shape[1])
    q[0] = 1
    XTX = np.matmul(X.transpose(), X)
    XTX_pow = XTX

    Q_basis = [q, np.matmul(XTX, q)]
    for i in range(2, BISECT_S):
        XTX_pow = np.matmul(XTX_pow, XTX)
        Q_basis.append(np.matmul(XTX_pow, q))
    Q = np.stack(Q_basis, axis=1)
    Q, _ = np.linalg.qr(Q)
    T = np.matmul(np.matmul(Q.transpose(), XTX), Q)
    eigs, eigvs = np.linalg.eigh(T)

    v = np.matmul(Q, eigvs[:, -1])
    margin = 0
    overlap = np.sort(np.abs(v))[:v.shape[0] // ALPHA]
    if len(overlap) > 0:
        margin = np.max(overlap)

    glue = (v >= -1 * margin) & (v < margin)
    return (
        v >= 0,
        glue,
        v < 0,
    )


def distance_key(i, j):
    if i <= j:
        return (i, j)
    else:
        return (j, i)


def knn_brute_force(X, k, indices):
    start = time.time()
    G = {}
    distances = {}
    for i in indices:
        for j in indices:
            if i < j:
                distances[distance_key(i, j)] = np.linalg.norm(
                    X[:, i] - X[:, j])
    for i in indices:
        neighbors = heapq.nsmallest(
            k,
            [(j, distances[distance_key(i, j)]) for j in indices if j != i],
            key=lambda pair: pair[1])
        G[i] = [j for (j, _) in neighbors]
    latency = time.time() - start
    log.debug("leaf size %d took %f seconds", len(indices), latency)
    if DEBUG:
        return G, distances, (latency, 1)
    else:
        return G, distances


@ray.remote
def knn(X_id, k, indices):
    X = ray.get(X_id[0])
    log.debug("iteration size: %d", len(indices))
    if len(indices) <= LEAF_SIZE:
        return knn_brute_force(X, k, indices)

    latency = 0
    start = time.time()
    X_subarray = center(X[:, indices])
    X_pos_condition, X_mid_condition, X_neg_condition = bisect(X_subarray)
    X_pos = indices[X_pos_condition]
    X_mid = indices[X_mid_condition]
    X_neg = indices[X_neg_condition]
    latency += time.time() - start
    log.debug("node bisect size %d took %f seconds", len(indices),
              time.time() - start)

    start = time.time()
    subtasks = [knn.remote(X_id, k, X_pos), knn.remote(X_id, k, X_mid),
                knn.remote(X_id, k, X_neg)]
    latency += time.time() - start
    log.debug("recursion submit %d took %f seconds", len(indices),
              time.time() - start)

    knn_pos, knn_mid, knn_neg = ray.get(subtasks)

    start = time.time()
    distances = {**knn_pos[1], **knn_mid[1], **knn_neg[1]}
    G = knn_pos[0]
    G_mid = knn_mid[0]
    G_neg = knn_neg[0]
    for i in indices:
        neighbors = [
            (j, distances[distance_key(i, j)]) for j in
            (G.get(i, []) + G_mid.get(i, []) + G_neg.get(i, []))]
        G[i] = [j for (j, _) in
                sorted(neighbors, key=lambda pair: pair[1])[:k]]
    latency += time.time() - start
    log.debug("node conquer size %d took %f seconds", len(indices),
              time.time() - start)

    if DEBUG:
        latencies = knn_pos[2], knn_mid[2], knn_neg[2]
        num_tasks = sum(count for _, count in latencies)
        mean_latency = sum(subtask_latency * count for
                           subtask_latency, count in latencies)
        mean_latency = (mean_latency + latency) / (num_tasks + 1)
        return G, distances, (mean_latency, num_tasks + 1)
    else:
        return G, distances


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-file', type=str)
    parser.add_argument('--num-samples', type=int)
    parser.add_argument('--num-dimensions', type=int)
    parser.add_argument('--k', type=int, default=1)

    args = parser.parse_args()
    start = time.time()
    if args.data_file is None:
        assert args.num_dimensions is not None
        assert args.num_samples is not None
        X = np.random.rand(args.num_dimensions, args.num_samples)
    else:
        X = load_mnist(args.data_file)
        if args.num_samples is not None:
            X = X[:, :args.num_samples]
    log.info("loading data took %f seconds", time.time() - start)

    ray.init()
    time.sleep(1)

    start = time.time()
    X_id = ray.put(X)
    if DEBUG:
        _, _, task_latencies = ray.get(
            knn.remote([X_id], args.k, np.arange(X.shape[1])))
        log.info("%d tasks with mean latency %f", task_latencies[1],
                 task_latencies[0])
    else:
        ray.get(knn.remote([X_id], args.k, np.arange(X.shape[1])))
    log.info("Ray took %f seconds", time.time() - start)
