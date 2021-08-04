#pragma once

#include <cstddef>
#include <vector>
#include <limits.h>
#include <ostream>

#include "ray/common/id.h"
#include "ray/util/logging.h"

namespace ray {

struct Priority {
 public:
  Priority() : Priority(0) {}

  Priority(int64_t depth) {
    extend(depth + 1);
  }

  Priority(const std::vector<int> &s) : score(s) {}

  void extend(int64_t size) const;

  bool operator==(const Priority &rhs) const {
    rhs.extend(score.size());
    extend(rhs.score.size());
    return score == rhs.score;
  }

  bool operator!=(const Priority &rhs) const {
    return !(*this == rhs);
  }

  bool operator<(const Priority &rhs) const {
    rhs.extend(score.size());
    extend(rhs.score.size());

    for (int64_t i = score.size() - 1; i >= 0; i--) {
      if (score[i] < rhs.score[i]) {
        return true;
      }
      if (score[i] > rhs.score[i]) {
        return false;
      }
    }
    // All indices equal.
    return false;
  }

  bool operator<=(const Priority &rhs) const {
    rhs.extend(score.size());
    extend(rhs.score.size());

    for (int64_t i = score.size() - 1; i >= 0; i--) {
      if (score[i] < rhs.score[i]) {
        return true;
      }
      if (score[i] > rhs.score[i]) {
        return false;
      }
    }
    // All indices equal.
    return true;
  }

  int GetScore(int64_t depth) const {
    extend(depth + 1);
    return score[depth];
  }

  void SetScore(int64_t depth, int s) {
    extend(depth + 1);
    RAY_CHECK(score[depth] >= s);
    score[depth] = s;
  }

  mutable std::vector<int> score = {};
};

using TaskKey = std::pair<Priority, TaskID>;

std::ostream &operator<<(std::ostream &os, const Priority &p);

}  // namespace ray
