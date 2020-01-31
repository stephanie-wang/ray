#include "ray/core_worker/reference_count.h"

#include <vector>

#include "gtest/gtest.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"

namespace ray {

static const rpc::Address empty_borrower;
static const ReferenceCounter::ReferenceTable empty_refs;

class ReferenceCountTest : public ::testing::Test {
 protected:
  std::unique_ptr<ReferenceCounter> rc;
  virtual void SetUp() { rc = std::unique_ptr<ReferenceCounter>(new ReferenceCounter); }

  virtual void TearDown() {}
};

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  MockWorkerClient(ReferenceCounter &rc, const std::string &addr) : rc_(rc), task_id_(TaskID::ForFakeTask()) {
    address_.set_ip_address(addr);
    address_.set_raylet_id(ClientID::FromRandom().Binary());
    address_.set_worker_id(WorkerID::FromRandom().Binary());
  }

  // TODO: Make message receive and reply async.
  ray::Status WaitForRefRemoved(const rpc::WaitForRefRemovedRequest &request,
      const rpc::ClientCallback<rpc::WaitForRefRemovedReply> &callback) override {
    auto r = num_requests_;
    requests_[r] = {
      std::make_shared<rpc::WaitForRefRemovedReply>(),
      callback,
    };

    auto send_reply_callback = [this, r](Status status, std::function<void()> success,
                                      std::function<void()> failure) {
      requests_[r].second(status, *requests_[r].first);
    };
    auto borrower_callback = [=]() {
      rc_.HandleWaitForRefRemoved(ObjectID::FromBinary(request.object_id()), absl::optional<ObjectID>(), requests_[r].first.get(), send_reply_callback);
    };
    borrower_callbacks_[r] = borrower_callback;

    num_requests_++;
    return Status::OK();
  }

  void FlushBorrowerCallbacks() {
    for (auto &callback : borrower_callbacks_) {
      callback.second();
    }
    borrower_callbacks_.clear();
  }

  void PutSerializedObjectId(const ObjectID &object_id) {
    rc_.AddOwnedObject(object_id, task_id_, address_);
    rc_.AddLocalReference(object_id);
  }

  void WrapObjectId(const ObjectID outer_id, const ObjectID &inner_id) {
    rc_.AddOwnedObject(outer_id, task_id_, address_);
    rc_.WrapObjectId(outer_id, {inner_id});
    rc_.AddLocalReference(outer_id);
  }

  void GetSerializedObjectId(const ObjectID outer_id, const ObjectID &inner_id, const TaskID &owner_id, const rpc::Address &owner_address) {
    rc_.AddLocalReference(inner_id);
    rc_.AddBorrowedObject(outer_id, inner_id, owner_id, owner_address);
  }

  void ExecuteTaskWithArg(const ObjectID &arg_id, const ObjectID &inner_id, const TaskID &owner_id, const rpc::Address &owner_address) {
    // Add a sentinel reference to keep the argument ID in scope even though
    // the frontend won't have a reference.
    rc_.AddLocalReference(arg_id);
    GetSerializedObjectId(arg_id, inner_id, owner_id, owner_address);
  }

  void SubmitTaskWithArg(const ObjectID &arg_id) {
    rc_.AddSubmittedTaskReferences({arg_id});
  }

  ReferenceCounter::ReferenceTable FinishExecutingTask(const ObjectID &arg_id) {
    auto refs = rc_.PopBorrowerRefs(arg_id);
    // Remove the sentinel reference.
    auto it = refs.find(arg_id);
    if (it != refs.end()) {
      RAY_CHECK(it->second.local_ref_count > 0);
      it->second.local_ref_count--;
    }
    rc_.RemoveLocalReference(arg_id, nullptr);
    return refs;
  }

  void HandleSubmittedTaskFinished(const ObjectID &arg_id, const rpc::Address &borrower_address = empty_borrower, const ReferenceCounter::ReferenceTable &borrower_refs = empty_refs) {
    rc_.RemoveSubmittedTaskReferences({arg_id}, borrower_address, borrower_refs, nullptr);
  }

  // Global map from Worker ID -> MockWorkerClient.
  // Global map from Object ID -> owner worker ID, list of objects that it depends on, worker address that it's scheduled on.
  // Worker map of pending return IDs.

  // The ReferenceCounter at the "client".
  ReferenceCounter &rc_;
  TaskID task_id_;
  rpc::Address address_;
  std::unordered_map<int, std::function<void()>> borrower_callbacks_;
  std::unordered_map<int, std::pair<std::shared_ptr<rpc::WaitForRefRemovedReply>, rpc::ClientCallback<rpc::WaitForRefRemovedReply>>> requests_;
  int num_requests_ = 0;
};

// Tests basic incrementing/decrementing of direct/submitted task reference counts. An
// entry should only be removed once both of its reference counts reach zero.
TEST_F(ReferenceCountTest, TestBasic) {
  std::vector<ObjectID> out;

  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();

  // Local references.
  rc->AddLocalReference(id1);
  rc->AddLocalReference(id1);
  rc->AddLocalReference(id2);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveLocalReference(id2, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 1);
  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 2);
  out.clear();

  // Submitted task references.
  rc->AddSubmittedTaskReferences({id1});
  rc->AddSubmittedTaskReferences({id1, id2});
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  rc->RemoveSubmittedTaskReferences({id1}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveSubmittedTaskReferences({id2}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 1);
  rc->RemoveSubmittedTaskReferences({id1}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 2);
  out.clear();

  // Local & submitted task references.
  rc->AddLocalReference(id1);
  rc->AddSubmittedTaskReferences({id1, id2});
  rc->AddLocalReference(id2);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveSubmittedTaskReferences({id2}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveSubmittedTaskReferences({id1}, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 1);
  rc->RemoveLocalReference(id2, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 2);
  out.clear();
}

// Tests that we can get the owner address correctly for objects that we own,
// objects that we borrowed via a serialized object ID, and objects whose
// origin we do not know.
TEST_F(ReferenceCountTest, TestOwnerAddress) {
  auto object_id = ObjectID::FromRandom();
  TaskID task_id = TaskID::ForFakeTask();
  rpc::Address address;
  address.set_ip_address("1234");
  rc->AddOwnedObject(object_id, task_id, address);

  TaskID added_id;
  rpc::Address added_address;
  ASSERT_TRUE(rc->GetOwner(object_id, &added_id, &added_address));
  ASSERT_EQ(task_id, added_id);
  ASSERT_EQ(address.ip_address(), added_address.ip_address());

  auto object_id2 = ObjectID::FromRandom();
  task_id = TaskID::ForFakeTask();
  address.set_ip_address("5678");
  rc->AddOwnedObject(object_id2, task_id, address);
  ASSERT_TRUE(rc->GetOwner(object_id2, &added_id, &added_address));
  ASSERT_EQ(task_id, added_id);
  ASSERT_EQ(address.ip_address(), added_address.ip_address());

  auto object_id3 = ObjectID::FromRandom();
  ASSERT_FALSE(rc->GetOwner(object_id3, &added_id, &added_address));
  rc->AddLocalReference(object_id3);
  ASSERT_FALSE(rc->GetOwner(object_id3, &added_id, &added_address));
}

// Tests that the ref counts are properly integrated into the local
// object memory store.
TEST(MemoryStoreIntegrationTest, TestSimple) {
  ObjectID id1 = ObjectID::FromRandom().WithDirectTransportType();
  ObjectID id2 = ObjectID::FromRandom().WithDirectTransportType();
  uint8_t data[] = {1, 2, 3, 4, 5, 6, 7, 8};
  RayObject buffer(std::make_shared<LocalMemoryBuffer>(data, sizeof(data)), nullptr);

  auto rc = std::shared_ptr<ReferenceCounter>(new ReferenceCounter());
  CoreWorkerMemoryStore store(nullptr, rc);

  // Tests putting an object with no references is ignored.
  RAY_CHECK_OK(store.Put(buffer, id2));
  ASSERT_EQ(store.Size(), 0);

  // Tests ref counting overrides remove after get option.
  rc->AddLocalReference(id1);
  RAY_CHECK_OK(store.Put(buffer, id1));
  ASSERT_EQ(store.Size(), 1);
  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext ctx(WorkerType::WORKER, JobID::Nil());
  RAY_CHECK_OK(store.Get({id1}, /*num_objects*/ 1, /*timeout_ms*/ -1, ctx,
                         /*remove_after_get*/ true, &results));
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(store.Size(), 1);
}

TEST(DistributedReferenceCountTest, TestNoBorrow) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc([&](const std::string &addr, int port) { return borrower;});
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->PutSerializedObjectId(inner_id);
  owner->WrapObjectId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner_rc.GetReference(outer_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).NumBorrowers() == 0);

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->task_id_, owner->address_);
  // The borrower submits a task that depends on the inner object.
  borrower->SubmitTaskWithArg(inner_id);
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The borrower waits for the task to finish before returning to the owner.
  borrower->HandleSubmittedTaskFinished(inner_id);
  auto borrower_refs = borrower->FinishExecutingTask(outer_id);
  // Check that the borrower's ref count is now 0 for all objects.
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs);
  borrower->FlushBorrowerCallbacks();
  // Check that owner's ref count is now 0 for all objects.
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(outer_id));
}

TEST(DistributedReferenceCountTest, TestSimpleBorrower) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc([&](const std::string &addr, int port) { return borrower;});
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->PutSerializedObjectId(inner_id);
  owner->WrapObjectId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner_rc.GetReference(outer_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).NumBorrowers() == 0);

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->task_id_, owner->address_);
  // The borrower submits a task that depends on the inner object.
  borrower->SubmitTaskWithArg(inner_id);
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The borrower task returns to the owner without waiting for its submitted
  // task to finish.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id);
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));
  // Check that the borrower's ref count for inner_id > 0 because of the
  // pending task.
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));
  ASSERT_TRUE(borrower_rc.GetReference(inner_id).RefCount() > 0);

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs);
  borrower->FlushBorrowerCallbacks();
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() == 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).NumBorrowers() > 0);
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));

  // The task submitted by the borrower returns. Everyone's ref count should go
  // to 0.
  borrower->HandleSubmittedTaskFinished(inner_id);
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(outer_id));
}

TEST(DistributedReferenceCountTest, TestSimpleBorrowerReferenceRemoved) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc([&](const std::string &addr, int port) { return borrower;});
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->PutSerializedObjectId(inner_id);
  owner->WrapObjectId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner_rc.GetReference(outer_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).NumBorrowers() == 0);

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->task_id_, owner->address_);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The borrower task returns to the owner while still using inner_id.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id);
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));
  ASSERT_TRUE(borrower_rc.GetReference(inner_id).RefCount() > 0);

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs);
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() == 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).NumBorrowers() > 0);
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));

  // The borrower is no longer using inner_id, but it hasn't received the
  // message from the owner yet.
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // The borrower receives the owner's wait message. It should return a reply
  // to the owner immediately saying that it is no longer using inner_id.
  borrower->FlushBorrowerCallbacks();
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

TEST(DistributedReferenceCountTest, TestBorrowerTree) {
  ReferenceCounter borrower_rc1;
  auto borrower1 = std::make_shared<MockWorkerClient>(borrower_rc1, "1");
  ReferenceCounter borrower_rc2;
  auto borrower2 = std::make_shared<MockWorkerClient>(borrower_rc2, "2");
  ReferenceCounter owner_rc([&](const std::string &addr, int port) {
        if (addr == borrower1->address_.ip_address()) {
          return borrower1;
        } else {
          return borrower2;
        }
      });
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "3");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->PutSerializedObjectId(inner_id);
  owner->WrapObjectId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner_rc.GetReference(outer_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).NumBorrowers() == 0);

  // Borrower 1 is given a reference to the inner object.
  borrower1->ExecuteTaskWithArg(outer_id, inner_id, owner->task_id_, owner->address_);
  // The borrower submits a task that depends on the inner object.
  auto outer_id2 = ObjectID::FromRandom();
  borrower1->WrapObjectId(outer_id2, inner_id);
  borrower1->SubmitTaskWithArg(outer_id2);
  borrower_rc1.RemoveLocalReference(inner_id, nullptr);
  borrower_rc1.RemoveLocalReference(outer_id2, nullptr);
  ASSERT_TRUE(borrower_rc1.HasReference(inner_id));
  ASSERT_TRUE(borrower_rc1.HasReference(outer_id2));

  // The borrower task returns to the owner without waiting for its submitted
  // task to finish.
  auto borrower_refs = borrower1->FinishExecutingTask(outer_id);
  ASSERT_TRUE(borrower_rc1.HasReference(inner_id));
  ASSERT_TRUE(borrower_rc1.HasReference(outer_id2));
  ASSERT_FALSE(borrower_rc1.HasReference(outer_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower1->address_, borrower_refs);
  borrower1->FlushBorrowerCallbacks();
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() == 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).NumBorrowers() > 0);
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));

  // Borrower 2 starts executing. It is given a reference to the inner object
  // when it gets outer_id2 as an argument.
  borrower2->ExecuteTaskWithArg(outer_id2, inner_id, owner->task_id_, owner->address_);
  ASSERT_TRUE(borrower_rc2.HasReference(inner_id));
  // Borrower 2 finishes but it is still using inner_id.
  borrower_refs = borrower2->FinishExecutingTask(outer_id2);
  ASSERT_TRUE(borrower_rc2.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc2.HasReference(outer_id2));
  ASSERT_FALSE(borrower_rc2.HasReference(outer_id));

  ASSERT_FALSE(borrower_rc1.GetReference(inner_id).owned_by_us);
  ASSERT_TRUE(borrower_rc1.GetReference(outer_id2).owned_by_us);
  borrower1->HandleSubmittedTaskFinished(outer_id2, borrower2->address_, borrower_refs);
  borrower2->FlushBorrowerCallbacks();
  // Borrower 1 no longer has a reference to any objects.
  ASSERT_FALSE(borrower_rc1.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc1.HasReference(outer_id2));
  // The owner should now have borrower 2 in its count.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() == 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).NumBorrowers() > 0);

  borrower_rc2.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower_rc2.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

TEST(DistributedReferenceCountTest, TestNestedObjectNoBorrow) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc([&](const std::string &addr, int port) { return borrower;});
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto mid_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->PutSerializedObjectId(inner_id);
  owner->WrapObjectId(mid_id, inner_id);
  owner->WrapObjectId(outer_id, mid_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to mid_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(mid_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for all objects.
  ASSERT_TRUE(owner_rc.GetReference(outer_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(mid_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() > 0);

  // The borrower is given a reference to the middle object.
  borrower->ExecuteTaskWithArg(outer_id, mid_id, owner->task_id_, owner->address_);
  ASSERT_TRUE(borrower_rc.HasReference(mid_id));
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));

  // The borrower unwraps the inner object with ray.get.
  borrower->GetSerializedObjectId(mid_id, inner_id, owner->task_id_, owner->address_);
  borrower_rc.RemoveLocalReference(mid_id, nullptr);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));
  // The borrower's reference to inner_id goes out of scope.
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(borrower_rc.HasReference(mid_id));

  // The borrower task returns to the owner.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id);
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs);
  // Check that owner now has nothing in scope.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));
  ASSERT_FALSE(owner_rc.HasReference(mid_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

TEST(DistributedReferenceCountTest, TestNestedObject) {
  ReferenceCounter borrower_rc;
  auto borrower = std::make_shared<MockWorkerClient>(borrower_rc, "1");
  ReferenceCounter owner_rc([&](const std::string &addr, int port) { return borrower;});
  auto owner = std::make_shared<MockWorkerClient>(owner_rc, "2");

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto mid_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->PutSerializedObjectId(inner_id);
  owner->WrapObjectId(mid_id, inner_id);
  owner->WrapObjectId(outer_id, mid_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to mid_id.
  owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner_rc.RemoveLocalReference(outer_id, nullptr);
  owner_rc.RemoveLocalReference(mid_id, nullptr);
  owner_rc.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for all objects.
  ASSERT_TRUE(owner_rc.GetReference(outer_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(mid_id).RefCount() > 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() > 0);

  // The borrower is given a reference to the middle object.
  borrower->ExecuteTaskWithArg(outer_id, mid_id, owner->task_id_, owner->address_);
  ASSERT_TRUE(borrower_rc.HasReference(mid_id));
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));

  // The borrower unwraps the inner object with ray.get.
  borrower->GetSerializedObjectId(mid_id, inner_id, owner->task_id_, owner->address_);
  borrower_rc.RemoveLocalReference(mid_id, nullptr);
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));

  // The borrower task returns to the owner while still using inner_id.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id);
  ASSERT_FALSE(borrower_rc.HasReference(outer_id));
  ASSERT_FALSE(borrower_rc.HasReference(mid_id));
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));
  ASSERT_TRUE(borrower_rc.GetReference(inner_id).RefCount() > 0);

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(outer_id, borrower->address_, borrower_refs);
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner_rc.HasReference(inner_id));
  ASSERT_TRUE(owner_rc.GetReference(inner_id).RefCount() == 0);
  ASSERT_TRUE(owner_rc.GetReference(inner_id).NumBorrowers() > 0);
  // Check that owner's ref count for outer and mid are 0 since the borrower
  // task returned and there were no local references to outer_id.
  ASSERT_FALSE(owner_rc.HasReference(outer_id));
  ASSERT_FALSE(owner_rc.HasReference(mid_id));

  // The borrower receives the owner's wait message. It should return a reply
  // to the owner immediately saying that it is no longer using inner_id.
  borrower->FlushBorrowerCallbacks();
  ASSERT_TRUE(borrower_rc.HasReference(inner_id));
  ASSERT_TRUE(owner_rc.HasReference(inner_id));

  // The borrower is no longer using inner_id, but it hasn't received the
  // message from the owner yet.
  borrower_rc.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower_rc.HasReference(inner_id));
  ASSERT_FALSE(owner_rc.HasReference(inner_id));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
