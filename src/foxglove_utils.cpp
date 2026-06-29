#include <data_tamer_tools/foxglove_utils.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cmath>
#include <ctime>

namespace data_tamer_tools
{
foxglove::schemas::LocationFix::PositionCovarianceType toFoxgloveCovarianceType(uint8_t covariance_type)
{
    using CovType = foxglove::schemas::LocationFix::PositionCovarianceType;
    switch (covariance_type)
    {
        case sensor_msgs::msg::NavSatFix::COVARIANCE_TYPE_APPROXIMATED:
            return CovType::APPROXIMATED;
        case sensor_msgs::msg::NavSatFix::COVARIANCE_TYPE_DIAGONAL_KNOWN:
            return CovType::DIAGONAL_KNOWN;
        case sensor_msgs::msg::NavSatFix::COVARIANCE_TYPE_KNOWN:
            return CovType::KNOWN;
        default:
            return CovType::UNKNOWN;
    }
}

std::optional<foxglove::schemas::Timestamp> toFoxgloveTimestamp(const builtin_interfaces::msg::Time& stamp)
{
    if (stamp.sec == 0 && stamp.nanosec == 0)
    {
        return std::nullopt;
    }
    return foxglove::schemas::Timestamp{ static_cast<uint32_t>(stamp.sec), static_cast<uint32_t>(stamp.nanosec) };
}

std::optional<foxglove::schemas::Duration> toFoxgloveDuration(const builtin_interfaces::msg::Duration& duration)
{
    if (duration.sec == 0 && duration.nanosec == 0)
    {
        return std::nullopt;
    }
    return foxglove::schemas::Duration{ static_cast<int32_t>(duration.sec), static_cast<uint32_t>(duration.nanosec) };
}

foxglove::schemas::Vector3 toFoxgloveVector3(const geometry_msgs::msg::Vector3& v)
{
    return foxglove::schemas::Vector3{ v.x, v.y, v.z };
}

foxglove::schemas::Vector3 toFoxgloveVector3(const geometry_msgs::msg::Point& p)
{
    return foxglove::schemas::Vector3{ p.x, p.y, p.z };
}

foxglove::schemas::Point3 toFoxglovePoint3(const geometry_msgs::msg::Point& p)
{
    return foxglove::schemas::Point3{ p.x, p.y, p.z };
}

foxglove::schemas::Quaternion toFoxgloveQuaternion(const geometry_msgs::msg::Quaternion& q)
{
    return foxglove::schemas::Quaternion{ q.x, q.y, q.z, q.w };
}

foxglove::schemas::Pose toFoxglovePose(const geometry_msgs::msg::Pose& pose)
{
    foxglove::schemas::Pose out;
    out.position = toFoxgloveVector3(pose.position);
    out.orientation = toFoxgloveQuaternion(pose.orientation);
    return out;
}

foxglove::schemas::Color toFoxgloveColor(const std_msgs::msg::ColorRGBA& color)
{
    return foxglove::schemas::Color{ color.r, color.g, color.b, color.a };
}

foxglove::schemas::FrameTransform toFoxgloveFrameTransform(const geometry_msgs::msg::TransformStamped& transform)
{
    foxglove::schemas::FrameTransform out;
    out.timestamp = toFoxgloveTimestamp(transform.header.stamp);
    out.parent_frame_id = transform.header.frame_id;
    out.child_frame_id = transform.child_frame_id;
    out.translation = foxglove::schemas::Vector3{ transform.transform.translation.x, transform.transform.translation.y, transform.transform.translation.z };
    out.rotation =
        foxglove::schemas::Quaternion{ transform.transform.rotation.x, transform.transform.rotation.y, transform.transform.rotation.z, transform.transform.rotation.w };
    return out;
}

foxglove::schemas::LaserScan toFoxgloveLaserScan(const sensor_msgs::msg::LaserScan& msg, const std::string& fallback_frame_id)
{
    foxglove::schemas::LaserScan out;
    out.timestamp = toFoxgloveTimestamp(msg.header.stamp);
    out.frame_id = msg.header.frame_id.empty() ? fallback_frame_id : msg.header.frame_id;
    out.start_angle = msg.angle_min;
    out.end_angle = msg.angle_max;
    out.ranges.reserve(msg.ranges.size());
    for (float range : msg.ranges)
    {
        out.ranges.push_back(static_cast<double>(range));
    }
    out.intensities.reserve(msg.intensities.size());
    for (float intensity : msg.intensities)
    {
        out.intensities.push_back(static_cast<double>(intensity));
    }
    return out;
}

foxglove::schemas::LocationFix buildLocationFix(const sensor_msgs::msg::NavSatFix& msg, const std::string& frame_id, const foxglove::schemas::Color& color)
{
    foxglove::schemas::LocationFix fix{};
    if (msg.header.stamp.sec != 0 || msg.header.stamp.nanosec != 0)
    {
        foxglove::schemas::Timestamp ts{ static_cast<uint32_t>(msg.header.stamp.sec), static_cast<uint32_t>(msg.header.stamp.nanosec) };
        fix.timestamp = ts;
    }
    fix.frame_id = frame_id;
    fix.latitude = msg.latitude;
    fix.longitude = msg.longitude;
    fix.altitude = msg.altitude;
    fix.position_covariance = msg.position_covariance;
    fix.position_covariance_type = toFoxgloveCovarianceType(msg.position_covariance_type);
    fix.color = color;
    return fix;
}

foxglove::schemas::Color topicColor(const std::string& topic)
{
    static const std::array<std::array<double, 4>, 10> palette = { {
        { 0.976, 0.224, 0.224, 1.0 },
        { 0.992, 0.596, 0.0, 1.0 },
        { 0.953, 0.769, 0.188, 1.0 },
        { 0.0, 0.631, 0.522, 1.0 },
        { 0.0, 0.737, 0.831, 1.0 },
        { 0.259, 0.522, 0.957, 1.0 },
        { 0.415, 0.239, 0.603, 1.0 },
        { 1.0, 0.596, 0.659, 1.0 },
        { 0.4, 0.4, 0.4, 1.0 },
        { 0.2, 0.6, 0.6, 1.0 },
    } };

    const size_t idx = std::hash<std::string>{}(topic) % palette.size();
    foxglove::schemas::Color color{};
    color.r = palette[idx][0];
    color.g = palette[idx][1];
    color.b = palette[idx][2];
    color.a = palette[idx][3];
    return color;
}

std::string joinTopicPrefix(const std::string& prefix, const std::string& topic)
{
    if (prefix.empty())
    {
        return topic;
    }
    if (topic.empty())
    {
        return prefix;
    }
    if (prefix.back() == '/' && topic.front() == '/')
    {
        return prefix + topic.substr(1);
    }
    if (prefix.back() != '/' && topic.front() != '/')
    {
        return prefix + "/" + topic;
    }
    return prefix + topic;
}

std::string markerEntityId(const visualization_msgs::msg::Marker& marker)
{
    if (!marker.ns.empty())
    {
        return marker.ns + ":" + std::to_string(marker.id);
    }
    return std::to_string(marker.id);
}

rclcpp::QoS navsatQoS(const std::string& qos_name)
{
    std::string lower = qos_name;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    if (lower == "sensor" || lower == "sensor_data" || lower == "sensordata")
    {
        return rclcpp::SensorDataQoS();
    }
    if (lower == "default" || lower == "system_default" || lower == "system")
    {
        return rclcpp::SystemDefaultsQoS();
    }
    if (lower == "best_effort")
    {
        return rclcpp::QoS(rclcpp::KeepLast(10)).best_effort();
    }
    if (lower == "reliable")
    {
        return rclcpp::QoS(rclcpp::KeepLast(10)).reliable();
    }
    return rclcpp::SensorDataQoS();
}

foxglove::McapCompression mcapCompression(const std::string& compression)
{
    std::string lower = compression;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    if (lower == "none")
    {
        return foxglove::McapCompression::None;
    }
    if (lower == "lz4")
    {
        return foxglove::McapCompression::Lz4;
    }
    return foxglove::McapCompression::Zstd;
}

std::string timestampSuffix()
{
    const auto sysnow = std::chrono::system_clock::now();
    const std::time_t tt = std::chrono::system_clock::to_time_t(sysnow);
    std::tm tm{};
    localtime_r(&tt, &tm);

    char buf[64];
    if (std::strftime(buf, sizeof(buf), "%Y-%m-%d_%H-%M-%S", &tm) == 0)
    {
        return "unknown";
    }
    return std::string(buf);
}

std::filesystem::path applyTimestamp(const std::filesystem::path& path)
{
    std::filesystem::path out = path;
    out.replace_filename(timestampSuffix() + "_" + out.filename().string());
    return out;
}
}  // namespace data_tamer_tools
