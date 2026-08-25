#pragma once

#include <foxglove/context.hpp>
#include <rclcpp/callback_group.hpp>
#include <rclcpp/duration.hpp>
#include <rclcpp/node.hpp>
#include <rclcpp/time.hpp>

#include <memory>
#include <string>
#include <string_view>

namespace data_tamer_tools
{
struct DataTamerRelayOptions
{
    const bool& use_protobuf;
};

struct DataTamerRelayCallbackGroups
{
    const rclcpp::CallbackGroup::SharedPtr& snapshot_callback_group;
};

class DataTamerRelayManager
{
  public:
    DataTamerRelayManager(rclcpp::Node& node, foxglove::Context& context, DataTamerRelayOptions options, DataTamerRelayCallbackGroups callback_groups);
    ~DataTamerRelayManager();

    DataTamerRelayManager(DataTamerRelayManager&&) noexcept;
    DataTamerRelayManager& operator=(DataTamerRelayManager&&) noexcept;

    DataTamerRelayManager(const DataTamerRelayManager&) = delete;
    DataTamerRelayManager& operator=(const DataTamerRelayManager&) = delete;

    bool ensureSourceForType(std::string_view ros_type, const std::string& topic);
    size_t softSweepIdleChannels(const rclcpp::Time& now, const rclcpp::Duration& ttl);
    size_t registrySize() const;

  private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace data_tamer_tools
