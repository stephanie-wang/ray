import heapq
import logging
import numpy as np
import time

import ray


logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)

LEAF_SIZE = 8
BISECT_S = 5
ALPHA = int(1 / 0.3)


def center(X):
    mean = X.mean(axis=1)
    return X - mean[:, np.newaxis]

def bisect(X):
    q = np.zeros(X.shape[1])
    q[0] = 1
    XTX = np.matmul(X.transpose(), X)

    Q = np.stack(
        [np.matmul(np.linalg.matrix_power(XTX, i), q) for i in
         range(BISECT_S)],
        axis=1)
    Q, _ = np.linalg.qr(Q)
    T = np.matmul(np.matmul(Q.transpose(), XTX), Q)
    eigs, eigvs = np.linalg.eigh(T)

    v = np.matmul(Q, eigvs[:, -1])
    margin = 0
    overlap = np.sort(np.abs(v))[:v.shape[0] // ALPHA]
    if len(overlap) > 0:
        margin = np.max(overlap)
    return v >= -1 * margin, v < margin

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
                distances[distance_key(i, j)] = np.linalg.norm(X[:, i] - X[:, j])
    for i in indices:
        neighbors = heapq.nsmallest(
            k,
            [(j, distances[distance_key(i, j)]) for j in indices if j != i],
            key=lambda pair: pair[1])
        G[i] = [j for (j, _) in neighbors]
    log.debug("leaf took %f seconds", time.time() - start)
    return G, distances

@ray.remote
def knn(X, k, indices):
    if len(indices) <= LEAF_SIZE:
        return knn_brute_force(X, k, indices)

    latency = 0
    start = time.time()
    X_subarray = center(X[:, indices])

    X_pos_condition, X_neg_condition= bisect(X_subarray)
    X_pos = indices[X_pos_condition]
    X_neg = indices[X_neg_condition]

    subtasks = [knn.remote(X, k, X_pos), knn.remote(X, k, X_neg)]
    latency += time.time() - start

    knn_pos, knn_neg = ray.get(subtasks)

    start = time.time()
    distances = {**knn_pos[1], **knn_neg[1]}
    G = knn_pos[0]
    G_neg = knn_neg[0]
    for i in indices:
        if i not in G:
            G[i] = G_neg[i]
        elif i in G_neg:
            neighbors = [(j, distances[distance_key(i, j)]) for j in G[i] + G_neg[i]]
            G[i] = [j for (j, _) in sorted(neighbors, key=lambda pair: pair[1])[:k]]
    latency += time.time() - start

    log.debug("node took %f seconds", latency)
    return G, distances

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-samples', type=int, required=True)
    parser.add_argument('--num-dimensions', type=int, required=True)
    parser.add_argument('--k', type=int, default=1)

    args = parser.parse_args()
    X = np.random.rand(args.num_dimensions, args.num_samples)

    ray.init()

    start = time.time()
    G, distances = ray.get(knn.remote(X, args.k, np.arange(args.num_samples)))
    log.info("Ray took %f seconds", time.time() - start)
