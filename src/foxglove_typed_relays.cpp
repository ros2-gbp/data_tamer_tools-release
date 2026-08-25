#include <data_tamer_tools/foxglove_typed_relays.hpp>

#include <data_tamer_tools/foxglove_utils.hpp>

#include <nlohmann/json.hpp>
#include <rclcpp/rclcpp.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <vector>

namespace data_tamer_tools
{
namespace
{
bool hasTransientLocalPublisher(rclcpp::Node& node, const std::string& topic)
{
    const std::vector<rclcpp::TopicEndpointInfo> publisher_infos = node.get_publishers_info_by_topic(topic);
    return std::any_of(publisher_infos.begin(), publisher_infos.end(),
                       [](const rclcpp::TopicEndpointInfo& info) { return info.qos_profile().durability() == rclcpp::DurabilityPolicy::TransientLocal; });
}

geometry_msgs::msg::Point transformPoint(const geometry_msgs::msg::Pose& pose, const geometry_msgs::msg::Point& point)
{
    const auto& q = pose.orientation;
    const double tx = 2.0 * (q.y * point.z - q.z * point.y);
    const double ty = 2.0 * (q.z * point.x - q.x * point.z);
    const double tz = 2.0 * (q.x * point.y - q.y * point.x);

    geometry_msgs::msg::Point transformed;
    transformed.x = point.x + q.w * tx + (q.y * tz - q.z * ty) + pose.position.x;
    transformed.y = point.y + q.w * ty + (q.z * tx - q.x * tz) + pose.position.y;
    transformed.z = point.z + q.w * tz + (q.x * ty - q.y * tx) + pose.position.z;
    return transformed;
}

std_msgs::msg::ColorRGBA markerPointColor(const visualization_msgs::msg::Marker& marker, const size_t index)
{
    if (marker.colors.size() == marker.points.size())
    {
        return marker.colors.at(index);
    }
    return marker.color;
}
}  // namespace

TypedRelayManager::TypedRelayManager(rclcpp::Node& node, foxglove::Context& context, TypedRelayOptions options, TypedRelayCallbackGroups callback_groups,
                                     TypedRelayRegistries registries)
  : node_(node), context_(context), options_(options), callback_groups_(callback_groups), registries_(registries)
{
}

bool TypedRelayManager::ensureSourceForType(std::string_view ros_type, const std::string& topic)
{
    using EnsureSourceFn = void (TypedRelayManager::*)(const std::string&);
    struct TypedDiscoveryRoute
    {
        std::string_view ros_type;
        EnsureSourceFn ensure_source;
    };

    static constexpr std::array<TypedDiscoveryRoute, 5> kTypedDiscoveryRoutes{ {
        { "sensor_msgs/msg/NavSatFix", &TypedRelayManager::ensureNavSatFixSource },
        { "geographic_msgs/msg/GeoPath", &TypedRelayManager::ensureGeoPathSource },
        { "sensor_msgs/msg/LaserScan", &TypedRelayManager::ensureLaserScanSource },
        { "visualization_msgs/msg/MarkerArray", &TypedRelayManager::ensureMarkerArraySource },
        { "tf2_msgs/msg/TFMessage", &TypedRelayManager::ensureTfMessageSource },
    } };

    for (const auto& route : kTypedDiscoveryRoutes)
    {
        if (ros_type != route.ros_type)
        {
            continue;
        }
        (this->*route.ensure_source)(topic);
        return true;
    }
    return false;
}

void TypedRelayManager::ensureNavSatFixSource(const std::string& topic)
{
    if (registries_.nsf_registry.has(topic))
    {
        return;
    }

    RCLCPP_INFO(node_.get_logger(), "Discovered new sensor_msgs/msg/NavSatFix source at %s", topic.c_str());
    NavSatFixSource::SharedPtr source = std::make_shared<NavSatFixSource>();
    source->topic = topic;
    source->foxglove_topic = joinTopicPrefix(options_.location_fix_prefix, topic);
    source->color = topicColor(source->foxglove_topic);

    auto channel = foxglove::messages::LocationFixChannel::create(source->foxglove_topic, context_);
    if (!channel.has_value())
    {
        RCLCPP_WARN(node_.get_logger(), "Failed to create LocationFix channel '%s': %s", source->foxglove_topic.c_str(), foxglove::strerror(channel.error()));
        return;
    }
    source->chan.emplace(std::move(channel.value()));
    source->last_seen_ns.store(node_.now().nanoseconds(), std::memory_order_relaxed);

    rclcpp::SubscriptionOptions navsat_options;
    navsat_options.callback_group = callback_groups_.navsat_callback_group;
    source->sub = node_.create_subscription<sensor_msgs::msg::NavSatFix>(
        topic, options_.navsat_qos, [this, source](sensor_msgs::msg::NavSatFix::ConstSharedPtr msg) { this->onNavSatFix(*msg, source); }, navsat_options);
    registries_.nsf_registry.emplace(source);
}

void TypedRelayManager::ensureGeoPathSource(const std::string& topic)
{
    if (registries_.geopath_registry.has(topic))
    {
        return;
    }

    RCLCPP_INFO(node_.get_logger(), "Discovered new geographic_msgs/msg/GeoPath source at %s", topic.c_str());
    GeoPathSource::SharedPtr source = std::make_shared<GeoPathSource>();
    source->topic = topic;
    source->foxglove_topic = joinTopicPrefix(options_.geojson_prefix, topic);
    source->color = topicColor(source->foxglove_topic);

    auto channel = foxglove::messages::GeoJSONChannel::create(source->foxglove_topic, context_);
    if (!channel.has_value())
    {
        RCLCPP_WARN(node_.get_logger(), "Failed to create GeoJSON channel '%s': %s", source->foxglove_topic.c_str(), foxglove::strerror(channel.error()));
        return;
    }
    source->chan.emplace(std::move(channel.value()));

    rclcpp::QoS qos = rclcpp::QoS(rclcpp::KeepLast(1)).reliable();
    if (hasTransientLocalPublisher(node_, topic))
    {
        qos.transient_local();
        RCLCPP_INFO(node_.get_logger(), "GeoPath source at %s offers transient-local durability; subscribing transient-local", topic.c_str());
    }

    rclcpp::SubscriptionOptions geopath_options;
    geopath_options.callback_group = callback_groups_.geopath_callback_group;
    source->sub = node_.create_subscription<geographic_msgs::msg::GeoPath>(
        topic, qos, [this, source](geographic_msgs::msg::GeoPath::ConstSharedPtr msg) { this->onGeoPath(*msg, source); }, geopath_options);
    registries_.geopath_registry.emplace(source);
}

void TypedRelayManager::replayLatchedGeoJson(uint64_t channel_id, std::optional<uint64_t> sink_id)
{
    for (const GeoPathSource::SharedPtr& source : registries_.geopath_registry.snapshot())
    {
        if (!source || !source->chan.has_value() || source->chan->id() != channel_id)
        {
            continue;
        }

        std::optional<foxglove::messages::GeoJSON> geojson;
        {
            std::scoped_lock lk(source->latest_mtx);
            geojson = source->latest_geojson;
        }

        if (!geojson.has_value())
        {
            RCLCPP_DEBUG(node_.get_logger(), "GeoJSON channel '%s' subscribed before a GeoPath sample was available", source->foxglove_topic.c_str());
            return;
        }

        const foxglove::FoxgloveError result = source->chan->log(geojson.value(), static_cast<uint64_t>(node_.now().nanoseconds()), sink_id);
        if (result != foxglove::FoxgloveError::Ok)
        {
            RCLCPP_WARN(node_.get_logger(), "Failed to replay GeoJSON on '%s': %s", source->foxglove_topic.c_str(), foxglove::strerror(result));
            return;
        }

        RCLCPP_INFO(node_.get_logger(), "Replayed latest GeoPath GeoJSON on '%s' to newly subscribed Foxglove client", source->foxglove_topic.c_str());
        return;
    }
}

void TypedRelayManager::ensureLaserScanSource(const std::string& topic)
{
    if (registries_.laser_scan_registry.has(topic))
    {
        return;
    }

    RCLCPP_INFO(node_.get_logger(), "Discovered new sensor_msgs/msg/LaserScan source at %s", topic.c_str());
    LaserScanSource::SharedPtr source = std::make_shared<LaserScanSource>();
    source->topic = topic;
    source->foxglove_topic = joinTopicPrefix(options_.laser_scan_prefix, topic);

    auto channel = foxglove::messages::LaserScanChannel::create(source->foxglove_topic, context_);
    if (!channel.has_value())
    {
        RCLCPP_WARN(node_.get_logger(), "Failed to create LaserScan channel '%s': %s", source->foxglove_topic.c_str(), foxglove::strerror(channel.error()));
        return;
    }
    source->chan.emplace(std::move(channel.value()));

    rclcpp::SubscriptionOptions laser_scan_options;
    laser_scan_options.callback_group = callback_groups_.laser_scan_callback_group;
    source->sub = node_.create_subscription<sensor_msgs::msg::LaserScan>(
        topic, rclcpp::SensorDataQoS(), [this, source](sensor_msgs::msg::LaserScan::ConstSharedPtr msg) { this->onLaserScan(*msg, source); }, laser_scan_options);
    registries_.laser_scan_registry.emplace(source);
}

void TypedRelayManager::ensureMarkerArraySource(const std::string& topic)
{
    if (registries_.marker_registry.has(topic))
    {
        return;
    }

    RCLCPP_INFO(node_.get_logger(), "Discovered new visualization_msgs/msg/MarkerArray source at %s", topic.c_str());
    MarkerArraySource::SharedPtr source = std::make_shared<MarkerArraySource>();
    source->topic = topic;
    source->foxglove_topic = joinTopicPrefix(options_.scene_prefix, topic);

    auto channel = foxglove::messages::SceneUpdateChannel::create(source->foxglove_topic, context_);
    if (!channel.has_value())
    {
        RCLCPP_WARN(node_.get_logger(), "Failed to create SceneUpdate channel '%s': %s", source->foxglove_topic.c_str(), foxglove::strerror(channel.error()));
        return;
    }
    source->chan.emplace(std::move(channel.value()));

    rclcpp::QoS qos = rclcpp::QoS(rclcpp::KeepLast(10)).reliable();
    rclcpp::SubscriptionOptions marker_options;
    marker_options.callback_group = callback_groups_.marker_callback_group;
    source->sub = node_.create_subscription<visualization_msgs::msg::MarkerArray>(
        topic, qos, [this, source](visualization_msgs::msg::MarkerArray::ConstSharedPtr msg) { this->onMarkerArray(*msg, source); }, marker_options);
    registries_.marker_registry.emplace(source);
}

void TypedRelayManager::ensureTfMessageSource(const std::string& topic)
{
    if (registries_.tf_registry.has(topic))
    {
        return;
    }

    RCLCPP_INFO(node_.get_logger(), "Discovered new tf2_msgs/msg/TFMessage source at %s", topic.c_str());
    TfMessageSource::SharedPtr source = std::make_shared<TfMessageSource>();
    source->topic = topic;
    source->foxglove_topic = joinTopicPrefix(options_.transform_prefix, topic);

    auto channel = foxglove::messages::FrameTransformChannel::create(source->foxglove_topic, context_);
    if (!channel.has_value())
    {
        RCLCPP_WARN(node_.get_logger(), "Failed to create FrameTransform channel '%s': %s", source->foxglove_topic.c_str(), foxglove::strerror(channel.error()));
        return;
    }
    source->chan.emplace(std::move(channel.value()));

    rclcpp::QoS qos = rclcpp::QoS(rclcpp::KeepLast(100)).reliable();
    if (topic == "/tf_static" || topic.ends_with("/tf_static"))
    {
        qos.transient_local();
    }
    rclcpp::SubscriptionOptions tf_options;
    tf_options.callback_group = callback_groups_.tf_callback_group;
    source->sub = node_.create_subscription<tf2_msgs::msg::TFMessage>(
        topic, qos, [this, source](tf2_msgs::msg::TFMessage::ConstSharedPtr msg) { this->onTfMessage(*msg, source); }, tf_options);
    registries_.tf_registry.emplace(source);
}

void TypedRelayManager::onNavSatFix(const sensor_msgs::msg::NavSatFix& msg, const NavSatFixSource::SharedPtr& source)
{
    if (!source || !source->chan.has_value())
    {
        return;
    }

    const rclcpp::Time now = node_.now();
    source->last_seen_ns.store(now.nanoseconds(), std::memory_order_relaxed);

    const rclcpp::Time stamp{ msg.header.stamp };
    const uint64_t log_time = (stamp.nanoseconds() > 0) ? static_cast<uint64_t>(stamp.nanoseconds()) : static_cast<uint64_t>(now.nanoseconds());

    const std::string frame = msg.header.frame_id.empty() ? source->topic : msg.header.frame_id;
    const foxglove::messages::LocationFix fix = buildLocationFix(msg, frame, source->color);

    const foxglove::FoxgloveError result = source->chan->log(fix, log_time);
    if (result != foxglove::FoxgloveError::Ok)
    {
        RCLCPP_WARN_THROTTLE(node_.get_logger(), *node_.get_clock(), 5000, "Failed to log LocationFix on '%s': %s", source->foxglove_topic.c_str(),
                             foxglove::strerror(result));
    }
}

std::string geoPathToGeoJson(const geographic_msgs::msg::GeoPath& msg, const std::string& topic, const foxglove::messages::Color& color, const rclcpp::Logger* logger)
{
    nlohmann::json feature_collection;
    feature_collection["type"] = "FeatureCollection";
    feature_collection["features"] = nlohmann::json::array();

    nlohmann::json coordinates = nlohmann::json::array();
    for (const auto& pose_stamped : msg.poses)
    {
        const double latitude = pose_stamped.pose.position.latitude;
        const double longitude = pose_stamped.pose.position.longitude;
        if (!std::isfinite(latitude) || !std::isfinite(longitude))
        {
            if (logger != nullptr)
            {
                RCLCPP_WARN(*logger, "Encountered infinite lat/lon in GeoPath '%s'", topic.c_str());
            }
            continue;
        }
        if (std::abs(latitude) > 90.0 || std::abs(longitude) > 180.0)
        {
            if (logger != nullptr)
            {
                RCLCPP_WARN(*logger, "Encountered invalid lat/lon in GeoPath '%s'", topic.c_str());
            }
            continue;
        }
        coordinates.push_back(nlohmann::json::array({ longitude, latitude }));
    }

    nlohmann::json properties = nlohmann::json::object();
    properties["topic"] = topic;
    properties["frame_id"] = msg.header.frame_id;
    properties["color"] = nlohmann::json::array({ color.r, color.g, color.b, color.a });
    properties["n"] = coordinates.size();

    if (coordinates.size() >= 2)
    {
        nlohmann::json line;
        line["type"] = "Feature";
        line["properties"] = properties;
        line["properties"]["kind"] = "geopath";
        line["geometry"] = { { "type", "LineString" }, { "coordinates", coordinates } };
        feature_collection["features"].push_back(std::move(line));
    }

    if (coordinates.size() >= 1)
    {
        nlohmann::json start;
        start["type"] = "Feature";
        start["properties"] = properties;
        start["properties"]["kind"] = "start";
        start["geometry"] = { { "type", "Point" }, { "coordinates", coordinates.front() } };
        feature_collection["features"].push_back(std::move(start));

        nlohmann::json goal;
        goal["type"] = "Feature";
        goal["properties"] = properties;
        goal["properties"]["kind"] = "goal";
        goal["geometry"] = { { "type", "Point" }, { "coordinates", coordinates.back() } };
        feature_collection["features"].push_back(std::move(goal));
    }

    return feature_collection.dump();
}

void TypedRelayManager::onGeoPath(const geographic_msgs::msg::GeoPath& msg, const GeoPathSource::SharedPtr& source)
{
    if (!source || !source->chan.has_value())
    {
        return;
    }

    const rclcpp::Time now = node_.now();
    source->last_seen_ns.store(now.nanoseconds(), std::memory_order_relaxed);

    const rclcpp::Time stamp{ msg.header.stamp };
    const uint64_t log_time = (stamp.nanoseconds() > 0) ? static_cast<uint64_t>(stamp.nanoseconds()) : static_cast<uint64_t>(now.nanoseconds());

    foxglove::messages::GeoJSON out{};
    const auto logger = node_.get_logger();
    out.geojson = data_tamer_tools::geoPathToGeoJson(msg, source->topic, source->color, &logger);
    {
        std::scoped_lock lk(source->latest_mtx);
        source->latest_geojson = out;
    }

    const foxglove::FoxgloveError result = source->chan->log(out, log_time);
    if (result != foxglove::FoxgloveError::Ok)
    {
        RCLCPP_WARN_THROTTLE(node_.get_logger(), *node_.get_clock(), 5000, "Failed to log GeoPath as GeoJSON on '%s': %s", source->foxglove_topic.c_str(),
                             foxglove::strerror(result));
    }
    else
    {
        RCLCPP_INFO(node_.get_logger(), "Relayed GeoPath '%s' as GeoJSON '%s' (%zu poses, %zu bytes)", source->topic.c_str(), source->foxglove_topic.c_str(),
                    msg.poses.size(), out.geojson.size());
    }
}

void TypedRelayManager::onLaserScan(const sensor_msgs::msg::LaserScan& msg, const LaserScanSource::SharedPtr& source)
{
    if (!source || !source->chan.has_value())
    {
        return;
    }

    const rclcpp::Time now = node_.now();
    source->last_seen_ns.store(now.nanoseconds(), std::memory_order_relaxed);

    const rclcpp::Time stamp{ msg.header.stamp };
    const uint64_t log_time = (stamp.nanoseconds() > 0) ? static_cast<uint64_t>(stamp.nanoseconds()) : static_cast<uint64_t>(now.nanoseconds());

    const foxglove::messages::LaserScan out = toFoxgloveLaserScan(msg, source->topic);
    const foxglove::FoxgloveError result = source->chan->log(out, log_time);
    if (result != foxglove::FoxgloveError::Ok)
    {
        RCLCPP_WARN_THROTTLE(node_.get_logger(), *node_.get_clock(), 5000, "Failed to log LaserScan on '%s': %s", source->foxglove_topic.c_str(),
                             foxglove::strerror(result));
    }
}

std::optional<foxglove::messages::SceneEntity> sceneEntityFromMarker(const visualization_msgs::msg::Marker& marker, const std::string& default_frame,
                                                                     const rclcpp::Logger* logger, const rclcpp::Clock* clock)
{
    foxglove::messages::SceneEntity entity;
    entity.timestamp = toFoxgloveTimestamp(marker.header.stamp);
    entity.frame_id = marker.header.frame_id.empty() ? default_frame : marker.header.frame_id;
    entity.id = markerEntityId(marker);
    entity.lifetime = toFoxgloveDuration(marker.lifetime);
    entity.frame_locked = marker.frame_locked;

    switch (marker.type)
    {
        case visualization_msgs::msg::Marker::ARROW:
        {
            if (marker.points.size() >= 2)
            {
                foxglove::messages::LinePrimitive line;
                line.type = foxglove::messages::LinePrimitive::LineType::LINE_STRIP;
                line.pose = toFoxglovePose(marker.pose);
                line.thickness = std::max(marker.scale.x, 1.0e-4);
                line.color = toFoxgloveColor(marker.color);
                for (const auto& point : marker.points)
                {
                    line.points.push_back(toFoxglovePoint3(point));
                }
                entity.lines.push_back(std::move(line));
            }
            else
            {
                foxglove::messages::ArrowPrimitive arrow;
                arrow.pose = toFoxglovePose(marker.pose);
                arrow.shaft_length = std::max(marker.scale.x * 0.7, 1.0e-4);
                arrow.shaft_diameter = std::max(marker.scale.y, 1.0e-4);
                arrow.head_length = std::max(marker.scale.x - arrow.shaft_length, std::max(marker.scale.x * 0.3, 1.0e-4));
                arrow.head_diameter = std::max(marker.scale.z, std::max(marker.scale.y * 1.5, 1.0e-4));
                arrow.color = toFoxgloveColor(marker.color);
                entity.arrows.push_back(std::move(arrow));
            }
            return entity;
        }

        case visualization_msgs::msg::Marker::CUBE:
        {
            foxglove::messages::CubePrimitive cube;
            cube.pose = toFoxglovePose(marker.pose);
            cube.size = toFoxgloveVector3(marker.scale);
            cube.color = toFoxgloveColor(marker.color);
            entity.cubes.push_back(std::move(cube));
            return entity;
        }

        case visualization_msgs::msg::Marker::SPHERE:
        {
            foxglove::messages::SpherePrimitive sphere;
            sphere.pose = toFoxglovePose(marker.pose);
            sphere.size = toFoxgloveVector3(marker.scale);
            sphere.color = toFoxgloveColor(marker.color);
            entity.spheres.push_back(std::move(sphere));
            return entity;
        }

        case visualization_msgs::msg::Marker::SPHERE_LIST:
        {
            if (marker.points.empty())
            {
                return std::nullopt;
            }
            for (size_t i = 0; i < marker.points.size(); ++i)
            {
                foxglove::messages::SpherePrimitive sphere;
                geometry_msgs::msg::Pose pose = marker.pose;
                pose.position = transformPoint(marker.pose, marker.points.at(i));
                sphere.pose = toFoxglovePose(pose);
                sphere.size = toFoxgloveVector3(marker.scale);
                sphere.color = toFoxgloveColor(markerPointColor(marker, i));
                entity.spheres.push_back(std::move(sphere));
            }
            return entity;
        }

        case visualization_msgs::msg::Marker::CYLINDER:
        {
            foxglove::messages::CylinderPrimitive cylinder;
            cylinder.pose = toFoxglovePose(marker.pose);
            cylinder.size = toFoxgloveVector3(marker.scale);
            cylinder.bottom_scale = 1.0;
            cylinder.top_scale = 1.0;
            cylinder.color = toFoxgloveColor(marker.color);
            entity.cylinders.push_back(std::move(cylinder));
            return entity;
        }

        case visualization_msgs::msg::Marker::LINE_STRIP:
        case visualization_msgs::msg::Marker::LINE_LIST:
        {
            if (marker.points.empty())
            {
                return std::nullopt;
            }
            foxglove::messages::LinePrimitive line;
            line.type = marker.type == visualization_msgs::msg::Marker::LINE_LIST ? foxglove::messages::LinePrimitive::LineType::LINE_LIST :
                                                                                    foxglove::messages::LinePrimitive::LineType::LINE_STRIP;
            line.pose = toFoxglovePose(marker.pose);
            line.thickness = std::max(marker.scale.x, 1.0e-4);
            line.color = toFoxgloveColor(marker.color);
            for (const auto& point : marker.points)
            {
                line.points.push_back(toFoxglovePoint3(point));
            }
            entity.lines.push_back(std::move(line));
            return entity;
        }

        case visualization_msgs::msg::Marker::TEXT_VIEW_FACING:
        {
            if (marker.text.empty())
            {
                return std::nullopt;
            }
            foxglove::messages::TextPrimitive text;
            text.pose = toFoxglovePose(marker.pose);
            text.billboard = true;
            text.font_size = std::max(marker.scale.z, 1.0e-4);
            text.scale_invariant = false;
            text.color = toFoxgloveColor(marker.color);
            text.text = marker.text;
            entity.texts.push_back(std::move(text));
            return entity;
        }

        default:
            if (logger != nullptr && clock != nullptr)
            {
                RCLCPP_WARN_THROTTLE(*logger, *clock, 5000, "Unsupported Marker type %d on SceneUpdate relay", marker.type);
            }
            return std::nullopt;
    }
}

void TypedRelayManager::onMarkerArray(const visualization_msgs::msg::MarkerArray& msg, const MarkerArraySource::SharedPtr& source)
{
    if (!source || !source->chan.has_value())
    {
        return;
    }

    const rclcpp::Time now = node_.now();
    source->last_seen_ns.store(now.nanoseconds(), std::memory_order_relaxed);

    foxglove::messages::SceneUpdate update;
    uint64_t log_time = static_cast<uint64_t>(now.nanoseconds());
    const auto logger = node_.get_logger();
    const auto clock = node_.get_clock();

    for (const auto& marker : msg.markers)
    {
        const auto marker_timestamp = toFoxgloveTimestamp(marker.header.stamp);
        if (marker_timestamp.has_value())
        {
            log_time = static_cast<uint64_t>(marker.header.stamp.sec) * 1000000000ULL + static_cast<uint64_t>(marker.header.stamp.nanosec);
        }

        if (marker.action == visualization_msgs::msg::Marker::DELETEALL)
        {
            foxglove::messages::SceneEntityDeletion deletion;
            deletion.timestamp = marker_timestamp;
            deletion.type = foxglove::messages::SceneEntityDeletion::SceneEntityDeletionType::ALL;
            update.deletions.push_back(std::move(deletion));
            continue;
        }

        if (marker.action == visualization_msgs::msg::Marker::DELETE)
        {
            foxglove::messages::SceneEntityDeletion deletion;
            deletion.timestamp = marker_timestamp;
            deletion.type = foxglove::messages::SceneEntityDeletion::SceneEntityDeletionType::MATCHING_ID;
            deletion.id = markerEntityId(marker);
            update.deletions.push_back(std::move(deletion));
            continue;
        }

        if (marker.action != visualization_msgs::msg::Marker::ADD)
        {
            RCLCPP_WARN_THROTTLE(node_.get_logger(), *node_.get_clock(), 5000, "Unsupported Marker action %d on SceneUpdate relay", marker.action);
            continue;
        }

        if (auto entity = data_tamer_tools::sceneEntityFromMarker(marker, source->topic, &logger, clock.get()); entity.has_value())
        {
            update.entities.push_back(std::move(entity.value()));
        }
    }

    if (update.deletions.empty() && update.entities.empty())
    {
        return;
    }

    const foxglove::FoxgloveError result = source->chan->log(update, log_time);
    if (result != foxglove::FoxgloveError::Ok)
    {
        RCLCPP_WARN_THROTTLE(node_.get_logger(), *node_.get_clock(), 5000, "Failed to log SceneUpdate on '%s': %s", source->foxglove_topic.c_str(),
                             foxglove::strerror(result));
    }
}

void TypedRelayManager::onTfMessage(const tf2_msgs::msg::TFMessage& msg, const TfMessageSource::SharedPtr& source)
{
    if (!source || !source->chan.has_value())
    {
        return;
    }

    const rclcpp::Time now = node_.now();
    source->last_seen_ns.store(now.nanoseconds(), std::memory_order_relaxed);

    if (msg.transforms.empty())
    {
        return;
    }

    for (const auto& transform : msg.transforms)
    {
        uint64_t log_time = static_cast<uint64_t>(now.nanoseconds());
        if (transform.header.stamp.sec != 0 || transform.header.stamp.nanosec != 0)
        {
            log_time = static_cast<uint64_t>(transform.header.stamp.sec) * 1000000000ULL + static_cast<uint64_t>(transform.header.stamp.nanosec);
        }
        const foxglove::messages::FrameTransform out = toFoxgloveFrameTransform(transform);
        const foxglove::FoxgloveError result = source->chan->log(out, log_time);
        if (result != foxglove::FoxgloveError::Ok)
        {
            RCLCPP_WARN_THROTTLE(node_.get_logger(), *node_.get_clock(), 5000, "Failed to log FrameTransform on '%s': %s", source->foxglove_topic.c_str(),
                                 foxglove::strerror(result));
        }
    }
}
}  // namespace data_tamer_tools
