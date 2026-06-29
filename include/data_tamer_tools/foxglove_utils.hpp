#pragma once

#include <builtin_interfaces/msg/duration.hpp>
#include <builtin_interfaces/msg/time.hpp>
#include <foxglove/foxglove.hpp>
#include <foxglove/mcap.hpp>
#include <geometry_msgs/msg/point.hpp>
#include <geometry_msgs/msg/pose.hpp>
#include <geometry_msgs/msg/quaternion.hpp>
#include <geometry_msgs/msg/transform_stamped.hpp>
#include <geometry_msgs/msg/vector3.hpp>
#include <rclcpp/qos.hpp>
#include <sensor_msgs/msg/laser_scan.hpp>
#include <sensor_msgs/msg/nav_sat_fix.hpp>
#include <std_msgs/msg/color_rgba.hpp>
#include <visualization_msgs/msg/marker.hpp>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

namespace data_tamer_tools
{
foxglove::schemas::LocationFix::PositionCovarianceType toFoxgloveCovarianceType(uint8_t covariance_type);

std::optional<foxglove::schemas::Timestamp> toFoxgloveTimestamp(const builtin_interfaces::msg::Time& stamp);
std::optional<foxglove::schemas::Duration> toFoxgloveDuration(const builtin_interfaces::msg::Duration& duration);

foxglove::schemas::Vector3 toFoxgloveVector3(const geometry_msgs::msg::Vector3& v);
foxglove::schemas::Vector3 toFoxgloveVector3(const geometry_msgs::msg::Point& p);
foxglove::schemas::Point3 toFoxglovePoint3(const geometry_msgs::msg::Point& p);
foxglove::schemas::Quaternion toFoxgloveQuaternion(const geometry_msgs::msg::Quaternion& q);
foxglove::schemas::Pose toFoxglovePose(const geometry_msgs::msg::Pose& pose);
foxglove::schemas::Color toFoxgloveColor(const std_msgs::msg::ColorRGBA& color);
foxglove::schemas::FrameTransform toFoxgloveFrameTransform(const geometry_msgs::msg::TransformStamped& transform);
foxglove::schemas::LaserScan toFoxgloveLaserScan(const sensor_msgs::msg::LaserScan& msg, const std::string& fallback_frame_id);
foxglove::schemas::LocationFix buildLocationFix(const sensor_msgs::msg::NavSatFix& msg, const std::string& frame_id, const foxglove::schemas::Color& color);

foxglove::schemas::Color topicColor(const std::string& topic);
std::string joinTopicPrefix(const std::string& prefix, const std::string& topic);
std::string markerEntityId(const visualization_msgs::msg::Marker& marker);

rclcpp::QoS navsatQoS(const std::string& qos_name);
foxglove::McapCompression mcapCompression(const std::string& compression);

std::string timestampSuffix();
std::filesystem::path applyTimestamp(const std::filesystem::path& path);
}  // namespace data_tamer_tools
