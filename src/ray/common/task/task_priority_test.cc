// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "gtest/gtest.h"
#include <limits.h>
#include "absl/container/btree_set.h"

#include "ray/common/common_protocol.h"
#include "ray/common/task/task_priority.h"

namespace ray {

TEST(TaskPriorityTest, TestEquals) {
  Priority priority1({1, 2, 3});
  Priority priority2({1, 2, 3});

  ASSERT_EQ(priority1, priority2);

  priority2.SetScore(3, INT_MAX);
  ASSERT_EQ(priority1, priority2);

  priority1.SetScore(4, INT_MAX);
  ASSERT_EQ(priority1, priority2);

  priority2.SetScore(3, 3);
  ASSERT_NE(priority1, priority2);
  priority1.SetScore(3, 3);
  ASSERT_EQ(priority1, priority2);
}

TEST(TaskPriorityTest, TestCompare) {
  Priority priority1({1, 2, 3});
  Priority priority2({1, 2});
  RAY_LOG(INFO) << priority1;
  RAY_LOG(INFO) << priority2;

  ASSERT_LE(priority1, priority1);
  ASSERT_LT(priority1, priority2);

  priority1.SetScore(3, 4);
  priority2.SetScore(3, 4);
  ASSERT_LT(priority1, priority2);

  priority2.SetScore(2, 2);
  ASSERT_LT(priority2, priority1);

  Priority priority3({});
  ASSERT_LT(priority1, priority3);
  ASSERT_LT(priority2, priority3);
}

TEST(TaskPriorityTest, TestSort) {
  std::set<Priority> queue;
  Priority p1({1, 2, 3});
  Priority p2({1, 2});
  Priority p3({});

  queue.insert(p1);
  queue.insert(p2);
  queue.insert(p3);
  {
    std::vector<Priority> expected_order({p1, p2, p3});
    for (auto &p : queue) {
      ASSERT_EQ(p, expected_order.front());
      expected_order.erase(expected_order.begin());
    }
  }

  queue.erase(p2);
  p2.SetScore(2, 2);

  queue.insert(p2);
  {
    std::vector<Priority> expected_order({p2, p1, p3});
    for (auto &p : queue) {
      ASSERT_EQ(p, expected_order.front());
      expected_order.erase(expected_order.begin());
    }
  }
}

TEST(TaskPriorityTest, TestBtree) {
  Priority p1({1, 2, 3});
  Priority p2({1, 2});
  Priority p3({});

  std::vector<std::pair<Priority, TaskID>> vec = {
    std::make_pair(p1, ObjectID::FromRandom().TaskId()),
    std::make_pair(p2, ObjectID::FromRandom().TaskId()),
    std::make_pair(p3, ObjectID::FromRandom().TaskId())
  };

  absl::btree_set<std::pair<Priority, TaskID>> set;
  for (auto &p : vec) {
    RAY_CHECK(set.emplace(p).second);
    RAY_CHECK(set.find(p) != set.end());
  }
  {
    for (auto &p : set) {
      ASSERT_EQ(p, vec.front());
      vec.erase(vec.begin());
    }
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
