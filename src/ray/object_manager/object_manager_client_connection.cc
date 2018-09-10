#include "ray/object_manager/object_manager_client_connection.h"

#include "ray/util/util.h"

namespace ray {

uint64_t SenderConnection::id_counter_;

void SenderConnection::Create(
    boost::asio::io_service &io_service, const ClientID &client_id, const std::string &ip,
    uint16_t port,
    const std::function<void(std::shared_ptr<SenderConnection>)> &callback) {
  boost::asio::ip::tcp::socket socket(io_service);
  auto endpoint = MakeTcpEndpoint(ip, port);
  std::shared_ptr<TcpServerConnection> conn =
      TcpServerConnection::Create(std::move(socket));
  uint64_t start = current_sys_time_ms();
  conn->ConnectAsync(endpoint, [callback, client_id, ip, start](std::shared_ptr<TcpServerConnection> connection) {
      if (connection) {
        auto sender_connection = std::make_shared<SenderConnection>(std::move(connection), client_id);
        callback(std::move(sender_connection));
      } else {
        callback(nullptr);
      }
      uint64_t end = current_sys_time_ms();
      uint64_t interval = end - start;
      if (interval > RayConfig::instance().handler_warning_timeout_ms()) {
        RAY_LOG(WARNING) << "HANDLER: TcpConnect to " << ip << " took " << interval << "ms";
      }
      });
};

SenderConnection::SenderConnection(std::shared_ptr<TcpServerConnection> conn,
                                   const ClientID &client_id)
    : conn_(conn) {
  client_id_ = client_id;
  connection_id_ = SenderConnection::id_counter_++;
};

}  // namespace ray
