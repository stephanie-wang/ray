#ifndef RAY_COMMON_CLIENT_CONNECTION_H
#define RAY_COMMON_CLIENT_CONNECTION_H

#include <memory>
#include <list>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "ray/id.h"
#include "ray/status.h"

namespace ray {

boost::asio::ip::tcp::endpoint MakeTcpEndpoint(const std::string &ip_address_string, int port);

/// \typename ServerConnection
///
/// A generic type representing a client connection to a server. This typename
/// can be used to write messages synchronously to the server.
template <typename T>
class ServerConnection : public std::enable_shared_from_this<ServerConnection<T>> {
 public:
  using endpoint_type = typename boost::asio::basic_stream_socket<T>::endpoint_type;

  static std::shared_ptr<ServerConnection<T>> Create(boost::asio::basic_stream_socket<T> &&socket);

  /// Write a message to the client.
  ///
  /// \param type The message type (e.g., a flatbuffer enum).
  /// \param length The size in bytes of the message.
  /// \param message A pointer to the message buffer.
  /// \return Status.
  ray::Status WriteMessage(int64_t type, int64_t length, const uint8_t *message);


  void WriteMessageAsync(int64_t type, int64_t length, const uint8_t *message,
      const std::function<void(const ray::Status&)> &handler);

  /// Write a buffer to this connection.
  ///
  /// \param buffer The buffer.
  /// \param ec The error code object in which to store error codes.
  Status WriteBuffer(const std::vector<boost::asio::const_buffer> &buffer);

  /// Read a buffer from this connection.
  ///
  /// \param buffer The buffer.
  /// \param ec The error code object in which to store error codes.
  void ReadBuffer(const std::vector<boost::asio::mutable_buffer> &buffer,
                  boost::system::error_code &ec);

  void ConnectAsync(
                       const endpoint_type &endpoint,
                       const std::function<void(std::shared_ptr<ServerConnection<T>>)> &callback);

 protected:
  struct WriteBufferData {
    int64_t write_version;
    int64_t write_type;
    uint64_t write_length;
    std::vector<uint8_t> write_message;
    std::function<void(const ray::Status&)> handler;
  };

  /// Create a connection to the server.
  ServerConnection(boost::asio::basic_stream_socket<T> &&socket);

  /// The socket connection to the server.
  boost::asio::basic_stream_socket<T> socket_;
  std::list<std::unique_ptr<WriteBufferData>> write_queue_;
  bool writing_;
  size_t max_messages_;
  bool connected_;

 private:

  void WriteSome();
};

template <typename T>
class ClientConnection;

template <typename T>
using ClientHandler = std::function<void(ClientConnection<T> &)>;
template <typename T>
using MessageHandler =
    std::function<void(std::shared_ptr<ClientConnection<T>>, int64_t, const uint8_t *)>;

/// \typename ClientConnection
///
/// A generic type representing a client connection on a server. In addition to
/// writing messages to the client, like in ServerConnection, this typename can
/// also be used to process messages asynchronously from client.
template <typename T>
class ClientConnection : public ServerConnection<T> {
 public:
  using std::enable_shared_from_this<ServerConnection<T>>::shared_from_this;
  /// Allocate a new node client connection.
  ///
  /// \param new_client_handler A reference to the client handler.
  /// \param message_handler A reference to the message handler.
  /// \param socket The client socket.
  /// \return std::shared_ptr<ClientConnection>.
  static std::shared_ptr<ClientConnection<T>> Create(
      ClientHandler<T> &new_client_handler, MessageHandler<T> &message_handler,
      boost::asio::basic_stream_socket<T> &&socket, const std::string &debug_label);

  std::shared_ptr<ClientConnection<T>> shared_ClientConnection_from_this() {
    return std::static_pointer_cast<ClientConnection<T>>(shared_from_this());
  }

  /// \return The ClientID of the remote client.
  const ClientID &GetClientID();

  /// \param client_id The ClientID of the remote client.
  void SetClientID(const ClientID &client_id);

  /// Listen for and process messages from the client connection. Once a
  /// message has been fully received, the client manager's
  /// ProcessClientMessage handler will be called.
  void ProcessMessages(bool sync = false);

  size_t Available();

 private:
  /// A private constructor for a node client connection.
  ClientConnection(MessageHandler<T> &message_handler,
                   boost::asio::basic_stream_socket<T> &&socket,
                   const std::string &debug_label);
  /// Process an error from the last operation, then process the  message
  /// header from the client.
  void ProcessMessageHeader(const boost::system::error_code &error, bool sync);
  /// Process an error from reading the message header, then process the
  /// message from the client.
  void ProcessMessage(const boost::system::error_code &error);

  /// The ClientID of the remote client.
  ClientID client_id_;
  /// The handler for a message from the client.
  MessageHandler<T> message_handler_;
  /// A label used for debug messages.
  const std::string debug_label_;
  /// Buffers for the current message being read rom the client.
  int64_t read_version_;
  int64_t read_type_;
  uint64_t read_length_;
  std::vector<uint8_t> read_message_;
	int64_t num_sync_messages_;
	int64_t sync_start_;
};

using LocalServerConnection = ServerConnection<boost::asio::local::stream_protocol>;
using TcpServerConnection = ServerConnection<boost::asio::ip::tcp>;
using LocalClientConnection = ClientConnection<boost::asio::local::stream_protocol>;
using TcpClientConnection = ClientConnection<boost::asio::ip::tcp>;

}  // namespace ray

#endif  // RAY_COMMON_CLIENT_CONNECTION_H
