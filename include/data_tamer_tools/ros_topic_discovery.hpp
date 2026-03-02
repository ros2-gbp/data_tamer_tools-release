#pragma once

#include <rclcpp/rclcpp.hpp>

#include <optional>
#include <string>
#include <string_view>

namespace data_tamer_tools
{
// Discover a ROS topic name by message type string (e.g. "pkg/msg/Msg").
//
// If multiple topics match, returns the lexicographically-smallest name and logs a warning.
// If no topics match, returns std::nullopt.
//
// Use this to avoid hard-coding topic names when a unique message type exists in the graph.
std::optional<std::string> discoverTopicByType(const rclcpp::Node& node, std::string_view ros_msg_type, const rclcpp::Logger& logger, std::string_view param_name = {});
}  // namespace data_tamer_tools
