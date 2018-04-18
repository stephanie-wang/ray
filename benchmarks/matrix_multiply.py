import numpy as np
from collections import namedtuple
import logging
import time

import ray


logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO)

LEAF_SIZE = 2 ** 8

MatrixBoundary = namedtuple("MatrixBoundary", ["start_row", "end_row", "start_column", "end_column"])

QUARTER_11 = (1, 1)
QUARTER_12 = (1, 2)
QUARTER_21 = (2, 1)
QUARTER_22 = (2, 2)

def get_boundary(shape, quarter):
    start_row, start_column = 0, 0
    end_row, end_column = shape

    if quarter[0] == 1:
        end_row -= end_row // 2
    else:
        start_row += end_row // 2

    if quarter[1] == 1:
        end_column -= end_column // 2
    else:
        start_column += end_column // 2
    return start_row, end_row, start_column, end_column

def get_matrix(matrix, quarter):
    start_row, end_row, start_column, end_column = get_boundary(matrix.shape, quarter)
    return matrix[
            start_row:end_row,
            start_column:end_column
            ]

def assign_matrix(matrix, quarter, value):
    start_row, end_row, start_column, end_column = get_boundary(matrix.shape, quarter)
    matrix[
            start_row:end_row,
            start_column:end_column
            ] = value

@ray.remote
def matmul(A, B):
    if A.shape[0] <= LEAF_SIZE:
        X = np.matmul(A, B)
        return X

    A_boundary = MatrixBoundary(0, A.shape[0], 0, A.shape[1])
    B_boundary = MatrixBoundary(0, B.shape[0], 0, B.shape[1])

    A11 = ray.put(get_matrix(A, QUARTER_11))
    A12 = ray.put(get_matrix(A, QUARTER_12))
    A21 = ray.put(get_matrix(A, QUARTER_21))
    A22 = ray.put(get_matrix(A, QUARTER_22))

    B11 = ray.put(get_matrix(B, QUARTER_11))
    B12 = ray.put(get_matrix(B, QUARTER_12))
    B21 = ray.put(get_matrix(B, QUARTER_21))
    B22 = ray.put(get_matrix(B, QUARTER_22))

    blocks = ray.get([
        matmul.remote(A11, B11),
        matmul.remote(A12, B21),
        matmul.remote(A11, B12),
        matmul.remote(A12, B22),
        matmul.remote(A21, B11),
        matmul.remote(A22, B21),
        matmul.remote(A21, B12),
        matmul.remote(A22, B22),
        ])

    C = np.empty((A.shape[0], B.shape[1]))

    assign_matrix(C, QUARTER_11, blocks[0] + blocks[1])
    assign_matrix(C, QUARTER_12, blocks[2] + blocks[3])
    assign_matrix(C, QUARTER_21, blocks[4] + blocks[5])
    assign_matrix(C, QUARTER_22, blocks[6] + blocks[7])

    return C

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--size', type=int, required=True)

    args = parser.parse_args()
    X = np.random.rand(args.size, args.size)

    ray.init(use_raylet=True)
    start = time.time()
    X_id = ray.put(X)
    ray_square = ray.get(matmul.remote(X_id, X_id))
    log.info("Ray took %f seconds", time.time() - start)

    start = time.time()
    np_square = np.matmul(X, X)
    log.info("numpy took %f seconds", time.time() - start)

    assert np.allclose(ray_square, np_square)
