#ifndef RAY_OBJECT_MANAGER_OBJECT_TRANSFER_H
#define RAY_OBJECT_MANAGER_OBJECT_TRANSFER_H


namespace ray {

static const auto transfer_timeout = boost::posix_time::milliseconds(1000);

class ObjectTransfer {
 public:
  ObjectTransfer(const ObjectID &object_id)
  : object_id_(object_id),
    clients_(),
    num_attempts_(0),
    transfer_requested_(false) {}

  bool AddClient(const ClientID &client_id) {
    auto it = std::find(clients_.begin(), clients_.end(), client_id);
    if (it == clients_.end()) {
      clients_.push_back(client_id);
      return true;
    } else {
      return false;
    }
  }

  bool TransferRequested() const { return transfer_requested_; }
  
  void StartTransfer() { transfer_requested_ = true; }

  ClientID RequestNextClient() {
    auto client = clients_[num_attempts_ % clients_.size()];
    num_attempts_++;
    return client;
  }

 private:

  const ObjectID object_id_;
  std::vector<ClientID> clients_;
  int num_attempts_;
  bool transfer_requested_;
};


}

#endif
