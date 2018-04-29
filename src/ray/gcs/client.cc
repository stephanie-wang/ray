#include "ray/gcs/client.h"

#include "ray/gcs/redis_context.h"

namespace {

int ParseIpAddrPort(const char *ip_addr_port, char *ip_addr, int *port) {
  char port_str[6];
  int parsed = sscanf(ip_addr_port, "%15[0-9.]:%5[0-9]", ip_addr, port_str);
  if (parsed != 2) {
    return -1;
  }
  *port = atoi(port_str);
  return 0;
}

void GetRedisShards(redisContext *context, std::vector<std::string> &db_shards_addresses,
                    std::vector<int> &db_shards_ports) {
  /* Get the total number of Redis shards in the system. */
  int num_attempts = 0;
  redisReply *reply = NULL;
  while (num_attempts < RayConfig::instance().redis_db_connect_retries()) {
    /* Try to read the number of Redis shards from the primary shard. If the
     * entry is present, exit. */
    reply = (redisReply *)redisCommand(context, "GET NumRedisShards");
    if (reply->type != REDIS_REPLY_NIL) {
      break;
    }

    /* Sleep for a little, and try again if the entry isn't there yet. */
    freeReplyObject(reply);
    usleep(RayConfig::instance().redis_db_connect_wait_milliseconds() * 1000);
    num_attempts++;
    continue;
  }
  RAY_CHECK(num_attempts < RayConfig::instance().redis_db_connect_retries())
      << "No entry found for NumRedisShards";
  RAY_CHECK(reply->type == REDIS_REPLY_STRING) << "Expected string, found Redis type "
                                               << reply->type << " for NumRedisShards";
  int num_redis_shards = atoi(reply->str);
  RAY_CHECK(num_redis_shards >= 1) << "Expected at least one Redis shard, "
                                   << "found " << num_redis_shards;
  freeReplyObject(reply);

  /* Get the addresses of all of the Redis shards. */
  num_attempts = 0;
  while (num_attempts < RayConfig::instance().redis_db_connect_retries()) {
    /* Try to read the Redis shard locations from the primary shard. If we find
     * that all of them are present, exit. */
    reply = (redisReply *)redisCommand(context, "LRANGE RedisShards 0 -1");
    if (static_cast<int>(reply->elements) == num_redis_shards) {
      break;
    }

    /* Sleep for a little, and try again if not all Redis shard addresses have
     * been added yet. */
    freeReplyObject(reply);
    usleep(RayConfig::instance().redis_db_connect_wait_milliseconds() * 1000);
    num_attempts++;
    continue;
  }
  RAY_CHECK(num_attempts < RayConfig::instance().redis_db_connect_retries())
      << "Expected " << num_redis_shards << " Redis shard addresses, found "
      << reply->elements;

  /* Parse the Redis shard addresses. */
  char db_shard_address[16];
  int db_shard_port;
  for (size_t i = 0; i < reply->elements; ++i) {
    /* Parse the shard addresses and ports. */
    RAY_CHECK(reply->element[i]->type == REDIS_REPLY_STRING);
    RAY_CHECK(ParseIpAddrPort(reply->element[i]->str, db_shard_address, &db_shard_port) ==
              0);
    db_shards_addresses.push_back(std::string(db_shard_address));
    db_shards_ports.push_back(db_shard_port);
  }
  freeReplyObject(reply);
}
}

namespace ray {

namespace gcs {

AsyncGcsClient::AsyncGcsClient(const ClientID &client_id, int num_shards,
                               bool use_task_shard)
    : num_shards_(num_shards), use_task_shard_(use_task_shard) {
  context_.reset(new RedisContext());
  if (num_shards_ == 0) {
    shard_contexts_.push_back(context_);
  } else {
    for (int i = 0; i < num_shards_; i++) {
      shard_contexts_.push_back(std::make_shared<RedisContext>());
    }
  }
  if (use_task_shard_) {
    auto task_table_context = std::move(shard_contexts_.front());
    shard_contexts_.erase(shard_contexts_.begin());
    task_table_contexts_.push_back(task_table_context);
  } else {
    task_table_contexts_ = shard_contexts_;
  }

  client_table_.reset(new ClientTable({context_}, this, client_id));
  object_table_.reset(new ObjectTable(shard_contexts_, this));
  actor_table_.reset(new ActorTable(shard_contexts_, this));
  task_table_.reset(new TaskTable(task_table_contexts_, this));
  raylet_task_table_.reset(new raylet::TaskTable(task_table_contexts_, this));
  task_reconstruction_log_.reset(new TaskReconstructionLog(task_table_contexts_, this));
  heartbeat_table_.reset(new HeartbeatTable({context_}, this));
}

AsyncGcsClient::AsyncGcsClient(const ClientID &client_id)
    : AsyncGcsClient(client_id, /*num_shards=*/0) {}

AsyncGcsClient::AsyncGcsClient() : AsyncGcsClient(ClientID::from_random()) {}

AsyncGcsClient::~AsyncGcsClient() {}

Status AsyncGcsClient::Connect(const std::string &address, int port) {
  RAY_RETURN_NOT_OK(context_->Connect(address, port));

  if (num_shards_ > 0) {
    redisContext *primary_context = context_->sync_context();
    std::vector<std::string> db_shards_addresses;
    std::vector<int> db_shards_ports;
    GetRedisShards(primary_context, db_shards_addresses, db_shards_ports);

    if (use_task_shard_) {
      RAY_CHECK(task_table_contexts_.size() == 1);
      const std::string task_table_address = db_shards_addresses.front();
      int task_table_port = db_shards_ports.front();
      db_shards_addresses.erase(db_shards_addresses.begin());
      db_shards_ports.erase(db_shards_ports.begin());
      RAY_RETURN_NOT_OK(
          task_table_contexts_[0]->Connect(task_table_address, task_table_port));
    }
    RAY_CHECK(db_shards_addresses.size() == shard_contexts_.size());
    for (size_t i = 0; i < shard_contexts_.size(); i++) {
      RAY_RETURN_NOT_OK(
          shard_contexts_[i]->Connect(db_shards_addresses[i], db_shards_ports[i]));
    }
  }

  // TODO(swang): Call the client table's Connect() method here. To do this,
  // we need to make sure that we are attached to an event loop first. This
  // currently isn't possible because the aeEventLoop, which we use for
  // testing, requires us to connect to Redis first.
  return Status::OK();
}

Status Attach(plasma::EventLoop &event_loop) {
  // TODO(pcm): Implement this via
  // context()->AttachToEventLoop(event loop)
  return Status::OK();
}

Status AsyncGcsClient::Attach(boost::asio::io_service &io_service) {
  asio_async_client_.reset(new RedisAsioClient(io_service, context_->async_context()));
  asio_subscribe_client_.reset(
      new RedisAsioClient(io_service, context_->subscribe_context()));

  if (use_task_shard_) {
    RAY_CHECK(task_table_contexts_.size() == 1);
    task_table_asio_async_client_.reset(
        new RedisAsioClient(io_service, task_table_contexts_[0]->async_context()));
    task_table_asio_subscribe_client_.reset(
        new RedisAsioClient(io_service, task_table_contexts_[0]->subscribe_context()));
  }

  if (num_shards_ > 0) {
    for (const auto &shard_context : shard_contexts_) {
      shard_asio_async_clients_.push_back(std::unique_ptr<RedisAsioClient>(
          new RedisAsioClient(io_service, shard_context->async_context())));
      shard_asio_async_clients_.push_back(std::unique_ptr<RedisAsioClient>(
          new RedisAsioClient(io_service, shard_context->subscribe_context())));
    }
  }

  return Status::OK();
}

ObjectTable &AsyncGcsClient::object_table() { return *object_table_; }

TaskTable &AsyncGcsClient::task_table() { return *task_table_; }

raylet::TaskTable &AsyncGcsClient::raylet_task_table() { return *raylet_task_table_; }

ActorTable &AsyncGcsClient::actor_table() { return *actor_table_; }

TaskReconstructionLog &AsyncGcsClient::task_reconstruction_log() {
  return *task_reconstruction_log_;
}

ClientTable &AsyncGcsClient::client_table() { return *client_table_; }

FunctionTable &AsyncGcsClient::function_table() { return *function_table_; }

ClassTable &AsyncGcsClient::class_table() { return *class_table_; }

HeartbeatTable &AsyncGcsClient::heartbeat_table() { return *heartbeat_table_; }

}  // namespace gcs

}  // namespace ray
