#include "client_connection.h"

#include <boost/bind.hpp>

#include "common.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/util/util.h"

namespace ray {

ray::Status TcpConnect(boost::asio::ip::tcp::socket &socket,
                       const std::string &ip_address_string, int port) {
  boost::asio::ip::address ip_address =
      boost::asio::ip::address::from_string(ip_address_string);
  boost::asio::ip::tcp::endpoint endpoint(ip_address, port);
  boost::system::error_code error;
  socket.connect(endpoint, error);
  return boost_to_ray_status(error);
}

template <class T>
ServerConnection<T>::ServerConnection(boost::asio::basic_stream_socket<T> &&socket)
    : socket_(std::move(socket)),
      write_queue_(),
      writing_(false),
      max_messages_(10) {}

template <class T>
Status ServerConnection<T>::WriteBuffer(
    const std::vector<boost::asio::const_buffer> &buffer) {
  boost::system::error_code error;
  // Loop until all bytes are written while handling interrupts.
  // When profiling with pprof, unhandled interrupts were being sent by the profiler to
  // the raylet process, which was causing synchronous reads and writes to fail.
  for (const auto &b : buffer) {
    uint64_t bytes_remaining = boost::asio::buffer_size(b);
    uint64_t position = 0;
    while (bytes_remaining != 0) {
      size_t bytes_written =
          socket_.write_some(boost::asio::buffer(b + position, bytes_remaining), error);
      position += bytes_written;
      bytes_remaining -= bytes_written;
      if (error.value() == EINTR) {
        continue;
      } else if (error.value() != boost::system::errc::errc_t::success) {
        return boost_to_ray_status(error);
      }
    }
  }
  return ray::Status::OK();
}

template <class T>
void ServerConnection<T>::ReadBuffer(
    const std::vector<boost::asio::mutable_buffer> &buffer,
    boost::system::error_code &ec) {
  // Loop until all bytes are read while handling interrupts.
  for (const auto &b : buffer) {
    uint64_t bytes_remaining = boost::asio::buffer_size(b);
    uint64_t position = 0;
    while (bytes_remaining != 0) {
      size_t bytes_read =
          socket_.read_some(boost::asio::buffer(b + position, bytes_remaining), ec);
      position += bytes_read;
      bytes_remaining -= bytes_read;
      if (ec.value() == EINTR) {
        continue;
      } else if (ec.value() != boost::system::errc::errc_t::success) {
        return;
      }
    }
  }
}

template <class T>
ray::Status ServerConnection<T>::WriteMessage(int64_t type, int64_t length,
                                              const uint8_t *message) {
  std::vector<boost::asio::const_buffer> message_buffers;
  auto write_version = RayConfig::instance().ray_protocol_version();
  message_buffers.push_back(boost::asio::buffer(&write_version, sizeof(write_version)));
  message_buffers.push_back(boost::asio::buffer(&type, sizeof(type)));
  message_buffers.push_back(boost::asio::buffer(&length, sizeof(length)));
  message_buffers.push_back(boost::asio::buffer(message, length));
  // Write the message and then wait for more messages.
  // TODO(swang): Does this need to be an async write?
  return WriteBuffer(message_buffers);
}

template <class T>
void ServerConnection<T>::WriteSome() {
  // Make sure we were not writing to the socket.
  RAY_CHECK(!writing_);
  writing_ = true;

  // Do an async write of everything currently in the queue to the socket.
  std::vector<boost::asio::const_buffer> message_buffers;
  size_t num_messages = 0;
  for (const auto &write_buffer : write_queue_) {
    message_buffers.push_back(boost::asio::buffer(&write_buffer->write_version, sizeof(write_buffer->write_version)));
    message_buffers.push_back(boost::asio::buffer(&write_buffer->write_type, sizeof(write_buffer->write_type)));
    message_buffers.push_back(boost::asio::buffer(&write_buffer->write_length, sizeof(write_buffer->write_length)));
    message_buffers.push_back(boost::asio::buffer(write_buffer->write_message));
    num_messages++;
    if (num_messages == max_messages_) {
      break;
    }
  }
  boost::asio::async_write(ServerConnection<T>::socket_, message_buffers,
      [this, num_messages](const boost::system::error_code &error, size_t
        bytes_transferred){
    ray::Status status = ray::Status::OK();
    if (error.value() != boost::system::errc::errc_t::success) {
      status = boost_to_ray_status(error);
    }
    // Call the handlers for the written messages.
    for (size_t i = 0; i < num_messages; i++) {
      auto write_buffer = std::move(write_queue_.front());
      write_buffer->handler(status);
      write_queue_.pop_front();
    }
    // We finished writing, so mark that we're no longer doing an async write.
    writing_ = false;
    // If there is more to write, try to write the rest.
    if (!write_queue_.empty()) {
      WriteSome();
    }
  });
}

template <class T>
void ServerConnection<T>::WriteMessageAsync(int64_t type, int64_t length, const
    uint8_t *message, const std::function<void(const ray::Status&)>
    &handler) {
  auto write_buffer = std::make_shared<WriteBufferData>();
  write_buffer->write_type = type;
  write_buffer->write_length = length;
  write_buffer->write_message.resize(length);
  write_buffer->write_message.assign(message, message + length);
  write_buffer->handler = handler;
  write_queue_.push_back(write_buffer);

  if (!writing_) {
    WriteSome();
  }
}


template <class T>
std::shared_ptr<ClientConnection<T>> ClientConnection<T>::Create(
    ClientHandler<T> &client_handler, MessageHandler<T> &message_handler,
    boost::asio::basic_stream_socket<T> &&socket, const std::string &debug_label) {
  std::shared_ptr<ClientConnection<T>> self(
      new ClientConnection(message_handler, std::move(socket), debug_label));
  // Let our manager process our new connection.
  client_handler(*self);
  return self;
}

template <class T>
ClientConnection<T>::ClientConnection(MessageHandler<T> &message_handler,
                                      boost::asio::basic_stream_socket<T> &&socket,
                                      const std::string &debug_label)
    : ServerConnection<T>(std::move(socket)),
      message_handler_(message_handler),
      debug_label_(debug_label),
      num_sync_messages_(0),
      sync_start_(current_time_ms()) {}

template <class T>
const ClientID &ClientConnection<T>::GetClientID() {
  return client_id_;
}

template <class T>
size_t ClientConnection<T>::Available() {
  return ServerConnection<T>::socket_.available();
}

template <class T>
void ClientConnection<T>::SetClientID(const ClientID &client_id) {
  client_id_ = client_id;
}

template <class T>
void ClientConnection<T>::ProcessMessages(bool sync) {
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<boost::asio::mutable_buffer> header;
  header.push_back(boost::asio::buffer(&read_version_, sizeof(read_version_)));
  header.push_back(boost::asio::buffer(&read_type_, sizeof(read_type_)));
  header.push_back(boost::asio::buffer(&read_length_, sizeof(read_length_)));
  if (num_sync_messages_ > 0 && (current_time_ms() - sync_start_) > 10) {
    // If we've been reading messages synchronously and it's been more than
    // 10ms since the first synchronous message, then go back to reading async.
    sync = false;
  }
  if (sync) {
    if (num_sync_messages_ == 0) {
      sync_start_ = current_time_ms();
    }
    num_sync_messages_++;
    boost::system::error_code error;
    boost::asio::read(
        ServerConnection<T>::socket_, header, error);
    ProcessMessageHeader(error, sync);
  } else {
    boost::asio::async_read(
        ServerConnection<T>::socket_, header,
        boost::bind(&ClientConnection<T>::ProcessMessageHeader, this->shared_from_this(),
                    boost::asio::placeholders::error, sync));
  }
}

template <class T>
void ClientConnection<T>::ProcessMessageHeader(const boost::system::error_code &error, bool sync) {
  if (!sync) {
    // An async call was requested.
    if (num_sync_messages_ > 1) {
      // If multiple synchronous calls were made, then log it.
      //RAY_LOG(INFO) << num_sync_messages_ << " ProcessMessages took " << current_time_ms() - sync_start_;
    }

    // Reset the number of sync calls.
    num_sync_messages_ = 0;
  }

  if (error) {
    // If there was an error, disconnect the client.
    read_type_ = static_cast<int64_t>(protocol::MessageType::DisconnectClient);
    read_length_ = 0;
    ProcessMessage(error);
    return;
  }

  // If there was no error, make sure the protocol version matches.
  RAY_CHECK(read_version_ == RayConfig::instance().ray_protocol_version());
  // Resize the message buffer to match the received length.
  read_message_.resize(read_length_);
  // Wait for the message to be read.
  if (sync) {
    boost::system::error_code error;
    boost::asio::read(
        ServerConnection<T>::socket_, boost::asio::buffer(read_message_), error);
    ProcessMessage(error);
  } else {
    boost::asio::async_read(
        ServerConnection<T>::socket_, boost::asio::buffer(read_message_),
        boost::bind(&ClientConnection<T>::ProcessMessage, this->shared_from_this(),
                    boost::asio::placeholders::error));
  }
}

template <class T>
void ClientConnection<T>::ProcessMessage(const boost::system::error_code &error) {
  if (error) {
    read_type_ = static_cast<int64_t>(protocol::MessageType::DisconnectClient);
  }

  uint64_t start_ms = current_time_ms();
  message_handler_(this->shared_from_this(), read_type_, read_message_.data());
  uint64_t interval = current_time_ms() - start_ms;
  if (interval > RayConfig::instance().handler_warning_timeout_ms()) {
    RAY_LOG(WARNING) << "[" << debug_label_ << "]ProcessMessage with type " << read_type_
                     << " took " << interval << " ms ";
  }
}

template class ServerConnection<boost::asio::local::stream_protocol>;
template class ServerConnection<boost::asio::ip::tcp>;
template class ClientConnection<boost::asio::local::stream_protocol>;
template class ClientConnection<boost::asio::ip::tcp>;

}  // namespace ray
