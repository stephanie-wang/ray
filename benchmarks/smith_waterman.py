import ray
import numpy as np
import itertools
import time
import sys


DEBUG = False

@ray.remote(num_returns=6)
def matrix(a, b, match_score=3, gap_cost=2, corner=0, prev_row=None, prev_col=None,
        a_start=0, b_start=0, chunk_dim=None):
    if DEBUG:
        print(a_start, b_start)
    start = time.time()

    if chunk_dim is None:
        chunk_dim = len(a)

    if prev_row is None:
        prev_row = np.zeros(chunk_dim, np.uint32)
    if prev_col is None:
        prev_col = np.zeros(chunk_dim, np.uint32)
    H = np.zeros((chunk_dim, chunk_dim), np.uint32)

    for i, j in itertools.product(range(0, H.shape[0]), range(0, H.shape[1])):
        if i == 0 and j == 0:
            match = corner
        elif i == 0:
            match = prev_row[j-1]
        elif j == 0:
            match = prev_col[i-1]
        else:
            match = H[i-1, j-1]
        if a[a_start+i] == b[b_start+j]:
            match += match_score

        if i == 0:
            delete = prev_row[j]
        else:
            delete = H[i - 1, j]
        if delete > gap_cost:
            delete -= gap_cost
        else:
            delete = 0

        if j == 0:
            insert = prev_col[i]
        else:
            insert = H[i, j - 1]
        if insert > gap_cost:
            insert -= gap_cost
        else:
            insert = 0

        H[i, j] = max(match, delete, insert)

    end = time.time()
    # Return the matrix, the bottom-right corner, the last row, and the last column.
    # Also return start time and end time.
    return H.max(), H[-1][-1], H[-1], H[:,-1], start, end

def matrix_blocked(a, b, num_chunks, match_score=3, gap_cost=2, debug=False):
    start = time.time()

    chunk_dim = len(a) // num_chunks
    assert len(a) % num_chunks == 0
    assert len(a) == len(b)

    a = ray.put(a)
    b = ray.put(b)

    H = [list(None for _ in range(num_chunks)) for _ in range(num_chunks)]

    for i in range(num_chunks * 2 - 1):
        for j in range(i + 1):
            x, y = j, i - j
            if x >= num_chunks or y >= num_chunks:
                continue

            if x == 0 or y == 0:
                corner = 0
            else:
                corner = H[x-1][y-1][1]

            if x == 0:
                prev_row = None
            else:
                prev_row = H[x-1][y][2]
            if y == 0:
                prev_col = None
            else:
                prev_col = H[x][y-1][3]
            H[x][y] = matrix.remote(a, b, match_score, gap_cost, corner,
                    prev_row, prev_col,
                    a_start=x*chunk_dim,
                    b_start=y*chunk_dim,
                    chunk_dim=chunk_dim)
        if DEBUG:
            print("Finished diagonal", i)

    starts = []
    ends = []
    for x in H:
        for _, _, _, _, s, e in x:
            starts.append(s)
            ends.append(e)

    for i, row in enumerate(H):
        H[i] = ray.get([m for m, _, _, _, _, _ in row])
    end = time.time()

    if debug:
        if num_chunks > 1:
            H = [np.concatenate(x, axis=1) for x in H]
            H = np.concatenate(H)
        print(H)

    starts = ray.get(starts)
    ends = ray.get(ends)
    task_durations = []
    for s, e in zip(starts, ends):
        task_durations.append(e - s)
    print("Average task duration", np.mean(task_durations))
    return (end - start), starts, ends


def traceback(H, b, b_='', old_i=0):
    # flip H to get index of **last** occurrence of H.max() with np.argmax()
    H_flip = np.flip(np.flip(H, 0), 1)
    i_, j_ = np.unravel_index(H_flip.argmax(), H_flip.shape)
    i, j = np.subtract(H.shape, (i_ + 1, j_ + 1))  # (i, j) are **last** indexes of H.max()
    if H[i, j] == 0:
        return b_, j
    b_ = b[j] + '-' + b_ if old_i - i > 1 else b[j] + b_
    return traceback(H[0:i, 0:j], b, b_, i)

def smith_waterman(a, b, match_score=3, gap_cost=2):
    a, b = a.upper(), b.upper()
    H = matrix_blocked(a, b, 2, match_score, gap_cost)
    b_, pos = traceback(H, b)
    pos += 1
    return pos, pos + len(b_)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--nbytes", default=100, type=int)
    parser.add_argument("--nchunks", default=1, type=int)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    import ray
    #ray.init(address="auto")
    ray.init()

    np.random.seed(0)

    chars = [b'G', b'T', b'A', b'C']
    a = np.random.choice(chars, size=args.nbytes)
    b = np.random.choice(chars, size=args.nbytes)
    print("a size", sys.getsizeof(a))
    print("b size", sys.getsizeof(b))
    total_time, starts, ends = matrix_blocked(a, b, args.nchunks, debug=args.debug)
    print(total_time)
