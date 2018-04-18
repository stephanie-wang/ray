import logging
import numpy as np
import scipy.spatial
import struct
import time

import ray


DEBUG = True

logging.basicConfig()
log = logging.getLogger(__name__)
if DEBUG:
    log.setLevel(logging.DEBUG)
else:
    log.setLevel(logging.INFO)

# Leaf size of 2 ** 5 yields 1ms per knn_brute_force call, including ray.put.
LEAF_SIZE = 2 ** 5
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
    return data / np.linalg.norm(data)


# Helper methods for matrix operations.
def center(X):
    mean = X.mean(axis=1)
    return X - mean[:, np.newaxis]


def bisect(X):
    q = np.zeros(X.shape[1])
    q[0] = 1
    Q_basis = [q]
    for i in range(1, BISECT_S):
        q = np.matmul(X, q)
        q = np.matmul(X.T, q)
        Q_basis.append(q)

    Q = np.stack(Q_basis, axis=1)
    Q, _ = np.linalg.qr(Q)
    XQ = np.matmul(X, Q)
    T = np.matmul(XQ.T, XQ)
    _, eigvs = np.linalg.eigh(T)

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
    X = X[:, indices]
    # distances is a hash table keyed by (i, j), where i and j are indices into
    # X, with i < j.
    distances = {}

    # Compute all pairwise distances with scipy.
    pdistances = scipy.spatial.distance.pdist(X.T)
    pdistances_square = scipy.spatial.distance.squareform(pdistances)

    for i in range(len(indices)):
        for j in range(len(indices)):
            if indices[i] < indices[j]:
                distances[(indices[i], indices[j])] = pdistances_square[i, j]

    # Get the k points that are closest to each point in the X subarray. Skip
    # the first element since this should be the point itself.
    index = np.argsort(pdistances_square)[:, 1:(k + 1)]
    G = indices[index]

    latency = time.time() - start
    log.debug("leaf size %d took %f seconds", len(indices), latency)
    return G, distances, (latency, 1)


@ray.remote
def knn(X_id, k, indices):
    if DEBUG:
        logging.basicConfig()
        log.setLevel(logging.DEBUG)

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

    G = np.zeros((len(indices), k))

    G_pos = knn_pos[0]
    G_mid = knn_mid[0]
    G_neg = knn_neg[0]

    g = np.zeros(3 * k)
    g_idx = np.zeros(3 * k)
    i_pos, i_mid, i_neg = 0, 0, 0
    for i, idx in enumerate(indices):
        g_idx.fill(-1)
        g.fill(np.inf)
        if X_pos_condition[i]:
            g_idx[:k] = G_pos[i_pos]
            g[:k] = [distances[distance_key(idx, j_idx)] for j_idx in
                     g_idx[:k]]
            i_pos += 1
        if X_mid_condition[i]:
            g_idx[k:2 * k] = G_mid[i_mid]
            g[k:2 * k] = [distances[distance_key(idx, j_idx)] for j_idx in
                          g_idx[k:2 * k]]
            i_mid += 1
        if X_neg_condition[i]:
            g_idx[2 * k:] = G_neg[i_neg]
            g[2 * k:] = [distances[distance_key(idx, j_idx)] for j_idx in
                         g_idx[2 * k:]]
            i_neg += 1
        _, nearest = np.unique(g, return_index=True)
        G[i] = g_idx[nearest[:k]]

    latency += time.time() - start
    log.debug("node conquer size %d took %f seconds", len(indices),
              time.time() - start)

    latencies = knn_pos[2], knn_mid[2], knn_neg[2]
    num_tasks = sum(count for _, count in latencies)
    mean_latency = sum(subtask_latency * count for
                       subtask_latency, count in latencies)
    mean_latency = (mean_latency + latency) / (num_tasks + 1)
    return G, distances, (mean_latency, num_tasks + 1)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-raylet', action='store_true')
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

    ray.init(use_raylet=args.use_raylet)
    time.sleep(10)

    log.info("Starting...")
    start = time.time()
    X_id = ray.put(X)
    _, _, task_latencies = ray.get(
        knn.remote([X_id], args.k, np.arange(X.shape[1])))
    log.info("%d tasks with mean latency %f", task_latencies[1],
             task_latencies[0])
    log.info("Ray took %f seconds", time.time() - start)
