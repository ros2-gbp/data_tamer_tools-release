#pragma once

#include <data_tamer_tools/foxglove_source_registry.hpp>

#include <foxglove/context.hpp>
#include <foxglove/schemas.hpp>
#include <geographic_msgs/msg/geo_path.hpp>
#include <rclcpp/callback_group.hpp>
#include <rclcpp/clock.hpp>
#include <rclcpp/logger.hpp>
#include <rclcpp/node.hpp>
#include <rclcpp/qos.hpp>
#include <sensor_msgs/msg/laser_scan.hpp>
#include <sensor_msgs/msg/nav_sat_fix.hpp>
#include <tf2_msgs/msg/tf_message.hpp>
#include <visualization_msgs/msg/marker.hpp>
#include <visualization_msgs/msg/marker_array.hpp>

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace data_tamer_tools
{
struct NavSatFixSource : TimedRelaySource<sensor_msgs::msg::NavSatFix, foxglove::schemas::LocationFixChannel>
{
    using SharedPtr = std::shared_ptr<NavSatFixSource>;

    foxglove::schemas::Color color;
};

using NavSatFixRegistry = TimedSourceRegistry<NavSatFixSource>;

struct GeoPathSource : TimedRelaySource<geographic_msgs::msg::GeoPath, foxglove::schemas::GeoJSONChannel>
{
    using SharedPtr = std::shared_ptr<GeoPathSource>;

    foxglove::schemas::Color color;
    std::mutex latest_mtx;
    std::optional<foxglove::schemas::GeoJSON> latest_geojson;
};

using GeoPathRegistry = TimedSourceRegistry<GeoPathSource>;

struct LaserScanSource : TimedRelaySource<sensor_msgs::msg::LaserScan, foxglove::schemas::LaserScanChannel>
{
    using SharedPtr = std::shared_ptr<LaserScanSource>;
};

using LaserScanRegistry = TimedSourceRegistry<LaserScanSource>;

struct MarkerArraySource : TimedRelaySource<visualization_msgs::msg::MarkerArray, foxglove::schemas::SceneUpdateChannel>
{
    using SharedPtr = std::shared_ptr<MarkerArraySource>;
};

using MarkerArrayRegistry = TimedSourceRegistry<MarkerArraySource>;

struct TfMessageSource : TimedRelaySource<tf2_msgs::msg::TFMessage, foxglove::schemas::FrameTransformChannel>
{
    using SharedPtr = std::shared_ptr<TfMessageSource>;
};

using TfMessageRegistry = TimedSourceRegistry<TfMessageSource>;

struct TypedRelayOptions
{
    const std::string& location_fix_prefix;
    const std::string& geojson_prefix;
    const std::string& laser_scan_prefix;
    const std::string& scene_prefix;
    const std::string& transform_prefix;
    const rclcpp::QoS& navsat_qos;
};

struct TypedRelayCallbackGroups
{
    const rclcpp::CallbackGroup::SharedPtr& navsat_callback_group;
    const rclcpp::CallbackGroup::SharedPtr& geopath_callback_group;
    const rclcpp::CallbackGroup::SharedPtr& laser_scan_callback_group;
    const rclcpp::CallbackGroup::SharedPtr& marker_callback_group;
    const rclcpp::CallbackGroup::SharedPtr& tf_callback_group;
};

struct TypedRelayRegistries
{
    NavSatFixRegistry& nsf_registry;
    GeoPathRegistry& geopath_registry;
    LaserScanRegistry& laser_scan_registry;
    MarkerArrayRegistry& marker_registry;
    TfMessageRegistry& tf_registry;
};

std::string geoPathToGeoJson(const geographic_msgs::msg::GeoPath& msg, const std::string& topic, const foxglove::schemas::Color& color,
                             const rclcpp::Logger* logger = nullptr);

std::optional<foxglove::schemas::SceneEntity> sceneEntityFromMarker(const visualization_msgs::msg::Marker& marker, const std::string& default_frame,
                                                                    const rclcpp::Logger* logger = nullptr, const rclcpp::Clock* clock = nullptr);

class TypedRelayManager
{
  public:
    TypedRelayManager(rclcpp::Node& node, foxglove::Context& context, TypedRelayOptions options, TypedRelayCallbackGroups callback_groups,
                      TypedRelayRegistries registries);

    bool ensureSourceForType(std::string_view ros_type, const std::string& topic);
    void replayLatchedGeoJson(uint64_t channel_id, std::optional<uint64_t> sink_id);

  private:
    void ensureNavSatFixSource(const std::string& topic);
    void ensureGeoPathSource(const std::string& topic);
    void ensureLaserScanSource(const std::string& topic);
    void ensureMarkerArraySource(const std::string& topic);
    void ensureTfMessageSource(const std::string& topic);

    void onNavSatFix(const sensor_msgs::msg::NavSatFix& msg, const NavSatFixSource::SharedPtr& source);
    void onGeoPath(const geographic_msgs::msg::GeoPath& msg, const GeoPathSource::SharedPtr& source);
    void onLaserScan(const sensor_msgs::msg::LaserScan& msg, const LaserScanSource::SharedPtr& source);
    void onMarkerArray(const visualization_msgs::msg::MarkerArray& msg, const MarkerArraySource::SharedPtr& source);
    void onTfMessage(const tf2_msgs::msg::TFMessage& msg, const TfMessageSource::SharedPtr& source);

    rclcpp::Node& node_;
    foxglove::Context& context_;
    TypedRelayOptions options_;
    TypedRelayCallbackGroups callback_groups_;
    TypedRelayRegistries registries_;
};
}  // namespace data_tamer_tools
