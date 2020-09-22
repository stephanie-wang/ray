import numpy as np
import time
import ray


def random_graph(dim):
    A = np.random.randint(0, 10, size=(dim, dim), dtype=np.int8)
    for i in range(dim):
        A[i,i] = 0
    return (A + A.T)

class Array:
    def __init__(self, chunks, num_chunks, chunk_dim):
        self.chunks = chunks
        self.num_chunks = num_chunks
        self.chunk_dim = chunk_dim

    def index(self, i, j):
        chunk_i, chunk_j = i // self.chunk_dim, j // self.chunk_dim
        chunk = self.chunks[chunk_i * self.num_chunks + chunk_j]
        return chunk[i % self.chunk_dim, j % self.chunk_dim]

    def to_array(self):
        rows = []
        for i in range(self.num_chunks):
            row = self.chunks[i*self.num_chunks:(i+1)*self.num_chunks]
            rows.append(np.concatenate(row, axis=1))
        return np.concatenate(rows)

@ray.remote
def floyd_warshall_block(row_start, row_dim, col_start, col_dim, k, num_chunks, *W):
    start = time.time()
    W = Array(W, num_chunks, row_dim)
    W_next = np.zeros((row_dim, col_dim), dtype=np.int8)

    for i in range(row_dim):
        for j in range(col_dim):
            if W.index(i+row_start, k) == np.iinfo(np.int8).max or W.index(k,j+col_start) == np.iinfo(np.int8).max:
                W_next[i,j] = W.index(i+row_start,j+col_start)
            else:
                W_next[i,j] = min(W.index(i+row_start,k) + W.index(k,j+col_start), W.index(i+row_start,j+col_start))
    end = time.time()
    #print(end - start)

    return W_next, (start, end)

@ray.remote
def floyd_warshall_row(v07, row_start, chunk_dim, k, num_chunks, *W):
    row = []
    for chunk_j in range(num_chunks):
        row.append(floyd_warshall_block.options(
            **({"num_return_vals": 2} if v07 else {"num_returns": 2})).remote(row_start, chunk_dim, chunk_j * chunk_dim, chunk_dim, k, num_chunks, *W))
    return row


def floyd_warshall(A, nchunks=None, use_single_driver=False, v07=False):
    N = len(A)
    if nchunks is None:
        nchunks = 1
    chunk_dim = N // nchunks
    assert N % chunk_dim == 0

    W = np.ones(A.shape, dtype=np.int8)
    W *= np.iinfo(np.int8).max
    for i in range(N):
        W[i,i] = 0
    W[A != 0] = A[A != 0]

    W_chunked = []
    for i in range(N // chunk_dim):
        for j in range(N // chunk_dim):
            chunk = ray.put(W[i*chunk_dim:(i+1)*chunk_dim,j*chunk_dim:(j+1)*chunk_dim])
            W_chunked.append(chunk)
    W = W_chunked

    all_intervals = []
    for k in range(N):
        start = time.time()
        chunks = []
        for chunk_i in range(N // chunk_dim):
            if use_single_driver:
                for chunk_j in range(N // chunk_dim):
                    chunks.append(floyd_warshall_block.options(
                        **({"num_return_vals": 2} if v07 else {"num_returns": 2})).remote(chunk_i * chunk_dim, chunk_dim, chunk_j * chunk_dim, chunk_dim, k, nchunks, *W))
            else:
                chunks.append(floyd_warshall_row.remote(v07, chunk_i * chunk_dim, chunk_dim, k, nchunks, *W))
        if not use_single_driver:
            chunks = ray.get(chunks)
            W = [chunk for row in chunks for chunk in row]
            W, intervals = zip(*W)
        else:
            W, intervals = zip(*chunks)

        W = list(W)
        intervals = list(intervals)
        all_intervals += intervals

        #ray.wait(W, num_returns=len(W))
        end = time.time()
        print("Round", k, "in", end - start, "seconds", len(W) / (end - start), "tasks/s")
    W = ray.get(W)
    return Array(W, nchunks, chunk_dim).to_array(), all_intervals


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--nbytes", default=100, type=int)
    parser.add_argument("--nchunks", default=1, type=int)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--single-driver", action="store_true")
    parser.add_argument("--output", default=None, type=str)
    parser.add_argument("--v07", action="store_true")
    args = parser.parse_args()

    ray.init()


    np.random.seed(0)
    A = random_graph(args.nbytes)
    start = time.time()
    W, intervals = floyd_warshall(A, args.nchunks, use_single_driver=args.single_driver, v07=args.v07)
    end = time.time()
    print("Took", end - start)

    if args.output:
        with open(args.output, 'w') as f:
            for s, e in ray.get(intervals):
                f.write('{} {}\n'.format(s, e))
