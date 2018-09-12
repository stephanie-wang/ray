#include <future>
#include <iostream>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "common/common.h"
#include "common/common_protocol.h"

#include "ray/object_manager/object_store_notification_manager.h"
#include "ray/util/util.h"

namespace ray {

void LogHandlerDelay(uint64_t start_ms, const std::string &operation, const TaskID &task_id, const ActorID &actor_id) {
  uint64_t end_ms = current_time_ms();
  uint64_t interval = end_ms - start_ms;
  if (interval > 100) {
    RAY_LOG(WARNING) << "HANDLER: " << operation << " on task " << task_id
                     << " for actor " << actor_id << " took " << interval << " ms ";
  }
}

ObjectStoreNotificationManager::ObjectStoreNotificationManager(
    boost::asio::io_service &io_service, const std::string &store_socket_name)
    : store_client_(), socket_(io_service) {
  ARROW_CHECK_OK(store_client_.Connect(store_socket_name.c_str(), "",
                                       plasma::kPlasmaDefaultReleaseDelay));

  ARROW_CHECK_OK(store_client_.Subscribe(&c_socket_));
  boost::system::error_code ec;
  socket_.assign(boost::asio::local::stream_protocol(), c_socket_, ec);
  assert(!ec.value());
  NotificationWait();
}

ObjectStoreNotificationManager::~ObjectStoreNotificationManager() {
  ARROW_CHECK_OK(store_client_.Disconnect());
}

void ObjectStoreNotificationManager::NotificationWait() {
  boost::asio::async_read(socket_, boost::asio::buffer(&length_, sizeof(length_)),
                          boost::bind(&ObjectStoreNotificationManager::ProcessStoreLength,
                                      this, boost::asio::placeholders::error));
}

void ObjectStoreNotificationManager::ProcessStoreLength(
    const boost::system::error_code &error) {
  notification_.resize(length_);
  boost::asio::async_read(
      socket_, boost::asio::buffer(notification_),
      boost::bind(&ObjectStoreNotificationManager::ProcessStoreNotification, this,
                  boost::asio::placeholders::error));
}

void ObjectStoreNotificationManager::ProcessStoreNotification(
    const boost::system::error_code &error) {
  if (error.value() != boost::system::errc::success) {
    RAY_LOG(FATAL) << boost_to_ray_status(error).ToString();
  }

  const auto &object_info = flatbuffers::GetRoot<ObjectInfo>(notification_.data());
  const auto &object_id = from_flatbuf(*object_info->object_id());
  if (object_info->is_deletion()) {
    ProcessStoreRemove(object_id);
  } else {
    ObjectInfoT result;
    object_info->UnPackTo(&result);
    ProcessStoreAdd(result);
  }
  NotificationWait();
}

void ObjectStoreNotificationManager::ProcessStoreAdd(const ObjectInfoT &object_info) {
  uint64_t start = current_time_ms();
  for (auto &handler : add_handlers_) {
    handler(object_info);
  }
  LogHandlerDelay(start, "ObjectAdded", ObjectID::from_binary(object_info.object_id), ActorID::nil());
}

void ObjectStoreNotificationManager::ProcessStoreRemove(const ObjectID &object_id) {
  uint64_t start = current_time_ms();
  for (auto &handler : rem_handlers_) {
    handler(object_id);
  }
  LogHandlerDelay(start, "ObjectRemoved", object_id, ActorID::nil());
}

void ObjectStoreNotificationManager::SubscribeObjAdded(
    std::function<void(const ObjectInfoT &)> callback) {
  add_handlers_.push_back(std::move(callback));
}

void ObjectStoreNotificationManager::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  rem_handlers_.push_back(std::move(callback));
}

}  // namespace ray
