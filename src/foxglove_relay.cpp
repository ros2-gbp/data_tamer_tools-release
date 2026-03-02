#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <geographic_msgs/msg/geo_path.hpp>
#include <rcl_interfaces/msg/log.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp/qos.hpp>
#include <rclcpp/serialization.hpp>
#include <rclcpp/serialized_message.hpp>

#include <data_tamer_msgs/msg/schema.hpp>
#include <data_tamer_msgs/msg/schemas.hpp>
#include <data_tamer_msgs/msg/snapshot.hpp>
#include <data_tamer_parser/data_tamer_parser.hpp>
#include <data_tamer_tools/helpers.hpp>
#include <data_tamer_tools/msg/log_dir.hpp>
#include <data_tamer_tools/ros_topic_discovery.hpp>

#include <foxglove/foxglove.hpp>
#include <foxglove/mcap.hpp>
#include <foxglove/server.hpp>
#include <sensor_msgs/msg/nav_sat_fix.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cctype>
#include <ctime>
#include <filesystem>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace data_tamer_tools
{

struct ChannelInfo
{
    std::string topic;                              // ROS2 topic (for reference)
    std::string json_schema;                        // JSON Schema text
    DataTamerParser::Schema parsed_schema;          // Parsed DataTamer schema
    std::shared_ptr<foxglove::RawChannel> channel;  // Foxglove channel (json)
    rclcpp::Time last_seen{ 0, 0 };
    ProtoRuntime proto_runtime;

    std::mutex pb_mutex;
    std::unique_ptr<google::protobuf::Message> m;
    std::unordered_map<std::string, const google::protobuf::FieldDescriptor*> field_by_name;
};

// Hash (uint64) -> ChannelInfo
class SchemaRegistry
{
  public:
    // Update last_seen for a specific hash
    void touch(uint64_t hash, const rclcpp::Time& t)
    {
        std::scoped_lock lk(m_);
        std::unordered_map<uint64_t, std::shared_ptr<ChannelInfo>>::iterator it = map_.find(hash);
        if (it != map_.end() && it->second)
        {
            it->second->last_seen = t;
        }
    }

    std::shared_ptr<ChannelInfo> get_mutable(uint64_t hash)
    {
        std::scoped_lock lk(m_);
        auto it = map_.find(hash);
        return (it == map_.end()) ? nullptr : it->second;
    }

    // Close channels that are idle past TTL, but DO NOT erase the entry.
    // Returns number of channels closed.
    size_t soft_sweep_idle_channels(const rclcpp::Time& now, const rclcpp::Duration& ttl)
    {
        std::scoped_lock lk(m_);
        size_t closed = 0;
        for (auto& [hash, info] : map_)
        {
            if (!info)
                continue;
            if (info->last_seen.nanoseconds() == 0)
                continue;  // never saw data
            const rclcpp::Duration age = now - info->last_seen;
            if (age > ttl)
            {
                if (info->channel)
                {
                    info->channel.reset();  // close Foxglove channel; keep schema & topic
                    ++closed;
                }
            }
        }
        return closed;
    }

    size_t size() const
    {
        std::scoped_lock lk(m_);
        return map_.size();
    }

    bool has(uint64_t hash) const
    {
        std::scoped_lock lk(m_);
        return map_.count(hash) != 0;
    }

    bool try_emplace(uint64_t h, std::shared_ptr<ChannelInfo> info)
    {
        std::scoped_lock lk(m_);
        std::pair<std::unordered_map<uint64_t, std::shared_ptr<ChannelInfo>>::iterator, bool> result = map_.emplace(h, std::move(info));
        return result.second;
    }

    void emplace(uint64_t hash, std::shared_ptr<ChannelInfo> info)
    {
        std::scoped_lock lk(m_);
        map_.emplace(hash, std::move(info));
    }

    std::shared_ptr<const ChannelInfo> get(uint64_t hash) const
    {
        std::scoped_lock lk(m_);
        std::unordered_map<uint64_t, std::shared_ptr<ChannelInfo>>::const_iterator it = map_.find(hash);
        return (it == map_.end()) ? nullptr : it->second;
    }

  private:
    mutable std::mutex m_;
    std::unordered_map<uint64_t, std::shared_ptr<ChannelInfo>> map_;
};

// --- DataTamer -> JSON helpers ----------------------------------------------

// --- Bridge node -------------------------------------------------------------
struct NavSatFixSource
{
    using SharedPtr = std::shared_ptr<NavSatFixSource>;
    std::string topic;
    std::string foxglove_topic;
    rclcpp::Subscription<sensor_msgs::msg::NavSatFix>::SharedPtr sub;
    std::optional<foxglove::schemas::LocationFixChannel> chan;
    foxglove::schemas::Color color;
    rclcpp::Time last_seen{ 0, 0 };
};

class NavSatFixRegistry
{
  public:
    bool has(const std::string& t)
    {
        std::scoped_lock lk(m);
        std::vector<NavSatFixSource::SharedPtr>::iterator it =
            std::find_if(sources.begin(), sources.end(), [&t](const NavSatFixSource::SharedPtr& s) { return s->topic == t; });
        return it != sources.end();
    }

    void emplace(NavSatFixSource::SharedPtr p)
    {
        std::scoped_lock lk(m);
        sources.push_back(p);
    }

    size_t prune(const rclcpp::Time& now, const rclcpp::Duration& ttl)
    {
        if (ttl.nanoseconds() <= 0)
        {
            return 0;
        }

        std::scoped_lock lk(m);
        const auto before = sources.size();
        sources.erase(std::remove_if(sources.begin(), sources.end(),
                                     [&](const NavSatFixSource::SharedPtr& s)
                                     {
                                         if (!s || !s->sub)
                                             return true;
                                         if (s->last_seen.nanoseconds() == 0)
                                             return false;  // never saw data
                                         return (now - s->last_seen) > ttl;
                                     }),
                      sources.end());
        return before - sources.size();
    }

  private:
    std::vector<NavSatFixSource::SharedPtr> sources;
    std::mutex m;
};

struct GeoPathSource
{
    using SharedPtr = std::shared_ptr<GeoPathSource>;
    std::string topic;
    std::string foxglove_topic;
    rclcpp::Subscription<geographic_msgs::msg::GeoPath>::SharedPtr sub;
    std::optional<foxglove::schemas::GeoJSONChannel> chan;
    foxglove::schemas::Color color;
    std::atomic<int64_t> last_seen_ns{ 0 };
};

class GeoPathRegistry
{
  public:
    bool has(const std::string& t)
    {
        std::scoped_lock lk(m);
        std::vector<GeoPathSource::SharedPtr>::iterator it =
            std::find_if(sources.begin(), sources.end(), [&t](const GeoPathSource::SharedPtr& s) { return s && s->topic == t; });
        return it != sources.end();
    }

    void emplace(GeoPathSource::SharedPtr p)
    {
        std::scoped_lock lk(m);
        sources.push_back(std::move(p));
    }

    size_t prune(int64_t now_ns, int64_t ttl_ns)
    {
        if (ttl_ns <= 0)
        {
            return 0;
        }

        std::scoped_lock lk(m);
        const auto before = sources.size();
        sources.erase(std::remove_if(sources.begin(), sources.end(),
                                     [&](const GeoPathSource::SharedPtr& s)
                                     {
                                         if (!s || !s->sub || !s->chan.has_value())
                                         {
                                             return true;
                                         }
                                         const int64_t last = s->last_seen_ns.load(std::memory_order_relaxed);
                                         if (last <= 0)
                                         {
                                             return false;  // never saw data
                                         }
                                         return (now_ns - last) > ttl_ns;
                                     }),
                      sources.end());
        return before - sources.size();
    }

  private:
    std::vector<GeoPathSource::SharedPtr> sources;
    std::mutex m;
};

class DtRos2ToFoxgloveBridge : public rclcpp::Node
{
  public:
    DtRos2ToFoxgloveBridge(const rclcpp::NodeOptions& o) : rclcpp::Node("foxglove_relay", o), context_(foxglove::Context::create())
    {
        using std::chrono::milliseconds;

        snapshot_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);
        rosout_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);
        timer_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);
        navsat_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::Reentrant);
        geopath_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::Reentrant);
        mcap_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);

        // Foxglove WebSocket server
        foxglove::WebSocketServerOptions opts;
        opts.context = context_;
        opts.host = declare_parameter<std::string>("host", "127.0.0.1");

        rcl_interfaces::msg::ParameterDescriptor port_description;
        port_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        port_description.name = "port";
        port_description.description = "Foxglove web socket port";
        port_description.integer_range.resize(1);
        port_description.integer_range[0].from_value = 0;
        port_description.integer_range[0].to_value = 65535;
        port_description.integer_range[0].step = 1;
        port_description.read_only = true;
        opts.port = declare_parameter(port_description.name, 8765, port_description);

        foxglove::FoxgloveResult<foxglove::WebSocketServer> serverResult = foxglove::WebSocketServer::create(std::move(opts));
        if (!serverResult.has_value())
        {
            RCLCPP_FATAL(get_logger(), "Foxglove server error: %s", foxglove::strerror(serverResult.error()));
            throw std::runtime_error("failed to create foxglove server");
        }
        server_.emplace(std::move(serverResult.value()));
        RCLCPP_INFO(get_logger(), "Foxglove WebSocket up at ws://%s:%u", opts.host.c_str(), server_->port());

        // Optional MCAP recording sink (writes selected Foxglove schemas to disk)
        enable_mcap_ = declare_parameter<bool>("enable_mcap", false);
        if (enable_mcap_)
        {
            mcap_dir_ = declare_parameter<std::string>("logdir", ".");
            mcap_filename_ = declare_parameter<std::string>("mcap_filename", "foxglove_relay.mcap");
            mcap_append_timestamp_ = declare_parameter<bool>("mcap_append_timestamp", true);
            mcap_profile_ = declare_parameter<std::string>("mcap_profile", "protobuf");
            mcap_truncate_ = declare_parameter<bool>("mcap_truncate", false);
            mcap_use_chunks_ = declare_parameter<bool>("mcap_use_chunks", true);
            mcap_chunk_size_ = static_cast<uint64_t>(std::max<int64_t>(0, declare_parameter<int64_t>("mcap_chunk_size", 0)));
            mcap_compression_ = mcapCompression(declare_parameter<std::string>("mcap_compression", "zstd"));

            rotate_dir_topic_ = declare_parameter<std::string>("rotate_dir_topic", "");

            if (mcap_filename_.empty())
            {
                mcap_filename_ = "foxglove_relay.mcap";
            }

            std::filesystem::path initial_path = std::filesystem::path(mcap_dir_) / mcap_filename_;
            if (mcap_append_timestamp_)
            {
                initial_path = applyTimestamp(initial_path);
            }

            if (!openMcapWriter(initial_path.string()))
            {
                RCLCPP_WARN(get_logger(), "Failed to start MCAP recording sink (enable_mcap=true)");
            }
            else
            {
                setupMcapRotationControl();
                if (!rotate_sub_ && rotate_dir_topic_.empty())
                {
                    RCLCPP_INFO(get_logger(), "rotate_dir_topic empty; waiting to auto-discover data_tamer_tools/msg/LogDir topic for MCAP rotation");
                    rotate_discovery_timer_ = create_wall_timer(std::chrono::seconds(1), [this]() { this->setupMcapRotationControl(); }, mcap_callback_group_);
                }
            }
        }

        rcl_interfaces::msg::ParameterDescriptor eviction_ttl_description;
        eviction_ttl_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        eviction_ttl_description.name = "eviction_ttl_sec";
        eviction_ttl_description.description = "Time to live for stale publishers";
        eviction_ttl_description.integer_range.resize(1);
        eviction_ttl_description.integer_range[0].from_value = 1;
        eviction_ttl_description.integer_range[0].to_value = 86400;  // 24 hours in seconds
        eviction_ttl_description.integer_range[0].step = 1;
        eviction_ttl_description.read_only = true;
        eviction_ttl_ = rclcpp::Duration::from_seconds(declare_parameter<int>("eviction_ttl_sec", 900, eviction_ttl_description));  // 15 min default

        rcl_interfaces::msg::ParameterDescriptor navsat_ttl_description;
        navsat_ttl_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        navsat_ttl_description.name = "navsat_ttl_sec";
        navsat_ttl_description.description = "Maximum age for NavSatFix sources relayed to Foxglove";
        navsat_ttl_description.integer_range.resize(1);
        navsat_ttl_description.integer_range[0].from_value = 1;
        navsat_ttl_description.integer_range[0].to_value = 3600;
        navsat_ttl_description.integer_range[0].step = 1;
        navsat_ttl_description.read_only = true;
        navsat_ttl_ = rclcpp::Duration::from_seconds(declare_parameter<int>("navsat_ttl_sec", 30, navsat_ttl_description));

        rcl_interfaces::msg::ParameterDescriptor geopath_ttl_description;
        geopath_ttl_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        geopath_ttl_description.name = "geopath_ttl_sec";
        geopath_ttl_description.description = "Maximum age for GeoPath sources relayed to Foxglove GeoJSON (0 to disable)";
        geopath_ttl_description.integer_range.resize(1);
        geopath_ttl_description.integer_range[0].from_value = 0;
        geopath_ttl_description.integer_range[0].to_value = 86400;  // 24 hours in seconds
        geopath_ttl_description.integer_range[0].step = 1;
        geopath_ttl_description.read_only = true;
        geopath_ttl_ = rclcpp::Duration::from_seconds(declare_parameter<int>("geopath_ttl_sec", 0, geopath_ttl_description));

        location_fix_prefix_ = declare_parameter<std::string>("location_fix_prefix", "/locations");
        geojson_prefix_ = declare_parameter<std::string>("geojson_prefix", "/geojson");

        const std::string navsat_qos_param = declare_parameter<std::string>("navsat_qos", "sensor");
        navsat_qos_ = navsatQoS(navsat_qos_param);

        rcl_interfaces::msg::ParameterDescriptor eviction_period_description;
        eviction_period_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        eviction_period_description.name = "eviction_period_sec";
        eviction_period_description.description = "Check for stale publishers every N seconds";
        eviction_period_description.integer_range.resize(1);
        eviction_period_description.integer_range[0].from_value = 1;
        eviction_period_description.integer_range[0].to_value = 86400;  // 24 hours in seconds
        eviction_period_description.integer_range[0].step = 1;
        eviction_period_description.read_only = true;
        // Eviction timer
        int eviction_period_sec = declare_parameter<int>("eviction_period_sec", 30, eviction_period_description);
        eviction_timer_ =
            create_wall_timer(std::chrono::seconds(eviction_period_sec), std::bind(&DtRos2ToFoxgloveBridge::evictStalePublishers, this), timer_callback_group_);

        rcl_interfaces::msg::ParameterDescriptor discovery_period_description;
        discovery_period_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        discovery_period_description.name = "discovery_sec";
        discovery_period_description.description = "Period for (re)discovering all Snapshot topics";
        discovery_period_description.integer_range.resize(1);
        discovery_period_description.integer_range[0].from_value = 1;
        discovery_period_description.integer_range[0].to_value = 60;  // 1 minute in seconds
        discovery_period_description.integer_range[0].step = 1;
        discovery_period_description.read_only = true;
        // Periodically (re)discover all Snapshot topics and subscribe dynamically.
        int discovery_sec = declare_parameter<int>("discovery_sec", 5, discovery_period_description);
        discovery_timer_ =
            create_wall_timer(std::chrono::seconds(discovery_sec), std::bind(&DtRos2ToFoxgloveBridge::discoverSnapshotTopics, this), timer_callback_group_);

        // --- ROSOUT options ---
        rcl_interfaces::msg::ParameterDescriptor rosout_enable_desc;
        rosout_enable_desc.type = rclcpp::ParameterType::PARAMETER_BOOL;
        rosout_enable_desc.name = "enable_rosout";
        rosout_enable_desc.description = "Relay /rosout to Foxglove using foxglove.schemas.Log";
        rosout_enable_desc.read_only = true;
        const bool enable_rosout = declare_parameter<bool>("enable_rosout", true, rosout_enable_desc);

        rcl_interfaces::msg::ParameterDescriptor rosout_topic_desc;
        rosout_topic_desc.type = rclcpp::ParameterType::PARAMETER_STRING;
        rosout_topic_desc.name = "rosout_topic";
        rosout_topic_desc.description = "ROS topic to subscribe for logs";
        rosout_topic_desc.read_only = true;
        const std::string rosout_topic = declare_parameter<std::string>("rosout_topic", "/rosout", rosout_topic_desc);

        if (enable_rosout)
        {
            // Create a typed Log channel (uses the built-in Log schema)
            auto chRes = foxglove::schemas::LogChannel::create("/rosout", context_);
            if (!chRes.has_value())
            {
                RCLCPP_WARN(get_logger(), "Failed to create LogChannel /rosout: %s", foxglove::strerror(chRes.error()));
            }
            else
            {
                rosout_chan_.emplace(std::move(chRes.value()));
                RCLCPP_INFO(get_logger(), "Relaying logs from '%s' to Foxglove Log channel)", rosout_topic.c_str());
                // Best-effort QoS is fine for logs
                auto qos = rclcpp::QoS(rclcpp::KeepLast(200)).best_effort();
                rclcpp::SubscriptionOptions rosout_options;
                rosout_options.callback_group = rosout_callback_group_;
                rosout_sub_ = create_subscription<rcl_interfaces::msg::Log>(
                    rosout_topic, qos, [this](rcl_interfaces::msg::Log::ConstSharedPtr msg) { this->onRosout(*msg); }, rosout_options);
            }
        }

        use_protobuf_ = declare_parameter<bool>("use_protobuf", true);
    }

    ~DtRos2ToFoxgloveBridge() override
    {
        closeMcapWriter();
    }

  private:
    void onSchemas(const data_tamer_msgs::msg::Schemas& msg)
    {
        for (const data_tamer_msgs::msg::Schema& s : msg.schemas) onSchema(s);
    }

    void onSchema(const data_tamer_msgs::msg::Schema& msg)
    {
        if (registry_.has(msg.hash))
        {
            registry_.touch(msg.hash, this->now());
            RCLCPP_INFO(get_logger(), "Schema already registered for hash=%lu", (unsigned long)msg.hash);
            return;
        }

        std::shared_ptr<ChannelInfo> info = std::make_shared<ChannelInfo>();
        info->parsed_schema = DataTamerParser::BuilSchemaFromText(msg.schema_text);
        info->topic = msg.channel_name;

        foxglove::Schema schema{};
        std::string message_encoding;
        if (use_protobuf_)
        {
            info->proto_runtime = data_tamer_tools::buildProto(info->parsed_schema);

            schema.encoding = "protobuf";
            schema.name = info->proto_runtime.full_type;
            schema.data = reinterpret_cast<const std::byte*>(info->proto_runtime.fdset_bytes.data());
            schema.data_len = info->proto_runtime.fdset_bytes.size();
            message_encoding = "protobuf";
        }
        else
        {
            info->json_schema = data_tamer_tools::convertToJSONSchema(info->parsed_schema).dump();
            schema.encoding = "jsonschema";
            schema.data = reinterpret_cast<const std::byte*>(info->json_schema.data());
            schema.data_len = info->json_schema.size();
            message_encoding = "json";
        }

        auto chanRes = foxglove::RawChannel::create(info->topic, message_encoding, std::move(schema), context_);
        if (!chanRes.has_value())
        {
            RCLCPP_ERROR(get_logger(), "Failed to create channel '%s'", info->topic.c_str());
            return;
        }
        info->channel = std::make_shared<foxglove::RawChannel>(std::move(chanRes.value()));
        info->last_seen = this->now();  // start the clock
        // Atomically publish it in the registry
        if (!registry_.try_emplace(msg.hash, info))
        {
            // Another thread won the race; just drop our extra channel.
            return;
        }

        RCLCPP_INFO(get_logger(), "Registered Foxglove channel '%s' (hash=%lu)", msg.channel_name.c_str(), (unsigned long)msg.hash);
    }

    // (Re)subscribe to every topic whose type == data_tamer_msgs/msg/Snapshot.
    void discoverSnapshotTopics()
    {
        // QoS: schemas are latched (Transient Local) so late-joiners get them once.
        rclcpp::QoS qos_schema = rclcpp::QoS(rclcpp::KeepLast(10)).transient_local().reliable();
        rclcpp::QoS qos_snap = rclcpp::QoS(rclcpp::KeepLast(100)).reliable();

        const std::map<std::string, std::vector<std::string>> topics = this->get_topic_names_and_types();
        rclcpp::SubscriptionOptions snapshot_options;
        snapshot_options.callback_group = snapshot_callback_group_;
        for (const auto& [name, types] : topics)
        {
            for (const auto& t : types)
            {
                // 1) Snapshots
                if (t == "data_tamer_msgs/msg/Snapshot")
                {
                    if (snapshot_subs_.count(name) == 0)
                    {
                        rclcpp::Subscription<data_tamer_msgs::msg::Snapshot>::SharedPtr sub = create_subscription<data_tamer_msgs::msg::Snapshot>(
                            name, qos_snap, [this, topic = name](data_tamer_msgs::msg::Snapshot::ConstSharedPtr msg) { this->onSnapshot(topic, *msg); },
                            snapshot_options);
                        snapshot_subs_.emplace(name, sub);
                        RCLCPP_INFO(get_logger(), "Subscribed to Snapshot: %s", name.c_str());
                    }
                }

                // 2) Single Schema
                if (t == "data_tamer_msgs/msg/Schema")
                {
                    if (schema_subs_.count(name) == 0)
                    {
                        rclcpp::Subscription<data_tamer_msgs::msg::Schema>::SharedPtr sub = create_subscription<data_tamer_msgs::msg::Schema>(
                            name, qos_schema, [this](data_tamer_msgs::msg::Schema::ConstSharedPtr msg) { this->onSchema(*msg); }, snapshot_options);
                        schema_subs_.emplace(name, sub);
                        RCLCPP_INFO(get_logger(), "Subscribed to Schema: %s", name.c_str());
                    }
                }

                // 3) Batch Schemas
                if (t == "data_tamer_msgs/msg/Schemas")
                {
                    if (schemas_subs_.count(name) == 0)
                    {
                        rclcpp::Subscription<data_tamer_msgs::msg::Schemas>::SharedPtr sub = create_subscription<data_tamer_msgs::msg::Schemas>(
                            name, qos_schema, [this](data_tamer_msgs::msg::Schemas::ConstSharedPtr msg) { this->onSchemas(*msg); }, snapshot_options);
                        schemas_subs_.emplace(name, sub);
                        RCLCPP_INFO(get_logger(), "Subscribed to Schemas: %s", name.c_str());
                    }
                }

                if (t == "sensor_msgs/msg/NavSatFix")
                {
                    if (!nsf_registry_.has(name))
                    {
                        RCLCPP_INFO(get_logger(), "Discovered new sensor_msgs/msg/NavSatFix source at %s", name.c_str());
                        NavSatFixSource::SharedPtr s = std::make_shared<NavSatFixSource>();
                        s->topic = name;
                        s->foxglove_topic = joinTopicPrefix(location_fix_prefix_, name);
                        s->color = topicColor(s->foxglove_topic);

                        auto ch = foxglove::schemas::LocationFixChannel::create(s->foxglove_topic, context_);
                        if (!ch.has_value())
                        {
                            RCLCPP_WARN(get_logger(), "Failed to create LocationFix channel '%s': %s", s->foxglove_topic.c_str(), foxglove::strerror(ch.error()));
                            continue;
                        }
                        s->chan.emplace(std::move(ch.value()));
                        s->last_seen = this->now();  // start the clock

                        rclcpp::SubscriptionOptions navsat_options;
                        navsat_options.callback_group = navsat_callback_group_;
                        s->sub = create_subscription<sensor_msgs::msg::NavSatFix>(
                            name, navsat_qos_, [this, source = s](sensor_msgs::msg::NavSatFix::ConstSharedPtr msg) { this->onNavSatFix(*msg, source); }, navsat_options);
                        nsf_registry_.emplace(s);
                    }
                }

                if (t == "geographic_msgs/msg/GeoPath")
                {
                    if (!geopath_registry_.has(name))
                    {
                        RCLCPP_INFO(get_logger(), "Discovered new geographic_msgs/msg/GeoPath source at %s", name.c_str());
                        GeoPathSource::SharedPtr s = std::make_shared<GeoPathSource>();
                        s->topic = name;
                        s->foxglove_topic = joinTopicPrefix(geojson_prefix_, name);
                        s->color = topicColor(s->foxglove_topic);

                        auto ch = foxglove::schemas::GeoJSONChannel::create(s->foxglove_topic, context_);
                        if (!ch.has_value())
                        {
                            RCLCPP_WARN(get_logger(), "Failed to create GeoJSON channel '%s': %s", s->foxglove_topic.c_str(), foxglove::strerror(ch.error()));
                            continue;
                        }
                        s->chan.emplace(std::move(ch.value()));

                        rclcpp::QoS qos = rclcpp::QoS(rclcpp::KeepLast(1)).reliable();
                        rclcpp::SubscriptionOptions geopath_options;
                        geopath_options.callback_group = geopath_callback_group_;
                        s->sub = create_subscription<geographic_msgs::msg::GeoPath>(
                            name, qos, [this, source = s](geographic_msgs::msg::GeoPath::ConstSharedPtr msg) { this->onGeoPath(*msg, source); }, geopath_options);
                        geopath_registry_.emplace(s);
                    }
                }
            }
        }
    }

    void onNavSatFix(const sensor_msgs::msg::NavSatFix& msg, const NavSatFixSource::SharedPtr& source)
    {
        if (!source || !source->chan.has_value())
        {
            return;
        }

        const rclcpp::Time now = this->now();
        source->last_seen = now;

        const rclcpp::Time stamp{ msg.header.stamp };
        const uint64_t log_time = (stamp.nanoseconds() > 0) ? static_cast<uint64_t>(stamp.nanoseconds()) : static_cast<uint64_t>(now.nanoseconds());

        const std::string frame = msg.header.frame_id.empty() ? source->topic : msg.header.frame_id;
        const foxglove::schemas::LocationFix fix = buildLocationFix(msg, frame, source->color);

        const foxglove::FoxgloveError result = source->chan->log(fix, log_time);
        if (result != foxglove::FoxgloveError::Ok)
        {
            RCLCPP_WARN_THROTTLE(get_logger(), *this->get_clock(), 5000, "Failed to log LocationFix on '%s': %s", source->foxglove_topic.c_str(),
                                 foxglove::strerror(result));
        }
    }

    foxglove::schemas::LocationFix::PositionCovarianceType toFoxgloveCovarianceType(uint8_t covariance_type) const
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

    foxglove::schemas::Color topicColor(const std::string& topic) const
    {
        static const std::array<std::array<double, 4>, 10> palette = { {
            { 0.976, 0.224, 0.224, 1.0 },  // vivid red
            { 0.992, 0.596, 0.0, 1.0 },    // orange
            { 0.953, 0.769, 0.188, 1.0 },  // amber
            { 0.0, 0.631, 0.522, 1.0 },    // teal
            { 0.0, 0.737, 0.831, 1.0 },    // cyan
            { 0.259, 0.522, 0.957, 1.0 },  // blue
            { 0.415, 0.239, 0.603, 1.0 },  // purple
            { 1.0, 0.596, 0.659, 1.0 },    // pink
            { 0.4, 0.4, 0.4, 1.0 },        // gray
            { 0.2, 0.6, 0.6, 1.0 },        // muted teal
        } };

        const size_t idx = std::hash<std::string>{}(topic) % palette.size();
        foxglove::schemas::Color color{};
        color.r = palette[idx][0];
        color.g = palette[idx][1];
        color.b = palette[idx][2];
        color.a = palette[idx][3];
        return color;
    }

    static std::string joinTopicPrefix(const std::string& prefix, const std::string& topic)
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

    rclcpp::QoS navsatQoS(const std::string& qos_name) const
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

    foxglove::McapCompression mcapCompression(const std::string& compression) const
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

    static std::string timestampSuffix()
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

    std::filesystem::path applyTimestamp(const std::filesystem::path& path) const
    {
        std::filesystem::path out = path;
        out.replace_filename(timestampSuffix() + "_" + out.filename().string());
        return out;
    }

    bool openMcapWriter(const std::string& path)
    {
        std::scoped_lock lk(mcap_mtx_);

        if (mcap_writer_.has_value())
        {
            const foxglove::FoxgloveError e = mcap_writer_->close();
            if (e != foxglove::FoxgloveError::Ok)
            {
                RCLCPP_WARN(get_logger(), "Failed to close MCAP writer: %s", foxglove::strerror(e));
            }
            mcap_writer_.reset();
        }

        mcap_path_ = path;  // keep backing storage alive for string_view in options
        const std::filesystem::path out_path{ mcap_path_ };
        if (!out_path.parent_path().empty())
        {
            std::error_code ec;
            std::filesystem::create_directories(out_path.parent_path(), ec);
            if (ec)
            {
                RCLCPP_WARN(get_logger(), "Failed to create MCAP directory '%s': %s", out_path.parent_path().string().c_str(), ec.message().c_str());
                return false;
            }
        }

        foxglove::McapWriterOptions o;
        o.context = context_;
        o.path = mcap_path_;
        o.profile = mcap_profile_;
        o.truncate = mcap_truncate_;
        o.compression = mcap_compression_;
        o.use_chunks = mcap_use_chunks_;
        if (mcap_chunk_size_ > 0)
        {
            o.chunk_size = mcap_chunk_size_;
        }
        o.sink_channel_filter = [](foxglove::ChannelDescriptor&& channel)
        {
            const auto schema = channel.schema();
            if (!schema)
            {
                return false;
            }
            const std::string_view schema_name = schema->name;
            return schema_name == "foxglove.LocationFix" || schema_name == "foxglove.GeoJSON";
        };

        auto r = foxglove::McapWriter::create(o);
        if (!r.has_value())
        {
            RCLCPP_WARN(get_logger(), "Failed to open MCAP '%s': %s", mcap_path_.c_str(), foxglove::strerror(r.error()));
            return false;
        }

        mcap_writer_.emplace(std::move(r.value()));
        RCLCPP_INFO(get_logger(), "Recording MCAP to '%s' (Foxglove schemas only)", mcap_path_.c_str());
        return true;
    }

    void setupMcapRotationControl()
    {
        if (!enable_mcap_ || rotate_sub_)
        {
            return;
        }

        std::string topic = rotate_dir_topic_;
        if (topic.empty())
        {
            std::optional<std::string> discovered = data_tamer_tools::discoverTopicByType(*this, "data_tamer_tools/msg/LogDir", get_logger(), "rotate_dir_topic");
            if (!discovered.has_value())
            {
                return;
            }
            topic = discovered.value();
            rotate_dir_topic_ = topic;
        }

        rclcpp::QoS qos(1);
        qos.transient_local().reliable();
        rclcpp::SubscriptionOptions rot_options;
        rot_options.callback_group = mcap_callback_group_;
        rotate_sub_ = create_subscription<data_tamer_tools::msg::LogDir>(
            topic, qos, [this](data_tamer_tools::msg::LogDir::ConstSharedPtr msg) { this->onRotateDir(*msg); }, rot_options);
        RCLCPP_INFO(get_logger(), "MCAP rotation enabled via topic '%s'", topic.c_str());

        if (rotate_discovery_timer_)
        {
            rotate_discovery_timer_->cancel();
        }
    }

    void onRotateDir(const data_tamer_tools::msg::LogDir& msg)
    {
        if (!enable_mcap_)
        {
            return;
        }
        if (msg.directory.empty())
        {
            RCLCPP_WARN(get_logger(), "RotateDir received empty directory string; ignoring");
            return;
        }

        std::error_code ec;
        std::filesystem::create_directories(msg.directory, ec);
        if (ec)
        {
            RCLCPP_ERROR(get_logger(), "Failed to create directory '%s': %s", msg.directory.c_str(), ec.message().c_str());
            return;
        }

        if (mcap_filename_.empty())
        {
            mcap_filename_ = "foxglove_relay.mcap";
        }

        std::filesystem::path next_path = std::filesystem::path(msg.directory) / mcap_filename_;
        if (mcap_append_timestamp_)
        {
            next_path = applyTimestamp(next_path);
        }

        if (!openMcapWriter(next_path.string()))
        {
            RCLCPP_ERROR(get_logger(), "MCAP rotate failed (dir='%s')", msg.directory.c_str());
        }
    }

    void closeMcapWriter()
    {
        std::scoped_lock lk(mcap_mtx_);
        if (!mcap_writer_.has_value())
        {
            return;
        }
        const foxglove::FoxgloveError e = mcap_writer_->close();
        if (e != foxglove::FoxgloveError::Ok)
        {
            RCLCPP_WARN(get_logger(), "Failed to close MCAP writer: %s", foxglove::strerror(e));
        }
        mcap_writer_.reset();
    }

    foxglove::schemas::LocationFix buildLocationFix(const sensor_msgs::msg::NavSatFix& msg, const std::string& frame_id, const foxglove::schemas::Color& color) const
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

    std::string geoPathToGeoJson(const geographic_msgs::msg::GeoPath& msg, const std::string& topic, const foxglove::schemas::Color& color) const
    {
        nlohmann::json fc;
        fc["type"] = "FeatureCollection";
        fc["features"] = nlohmann::json::array();

        nlohmann::json coords = nlohmann::json::array();
        for (const auto& ps : msg.poses)
        {
            const double lat = ps.pose.position.latitude;
            const double lon = ps.pose.position.longitude;
            if (!std::isfinite(lat) || !std::isfinite(lon))
            {
                RCLCPP_WARN(get_logger(), "Encountered infinite lat/lon in GeoPath '%s'", topic.c_str());
                continue;
            }
            if (std::abs(lat) > 90.0 || std::abs(lon) > 180.0)
            {
                RCLCPP_WARN(get_logger(), "Encountered invalid lat/lon in GeoPath '%s'", topic.c_str());
                continue;
            }
            coords.push_back(nlohmann::json::array({ lon, lat }));
        }

        nlohmann::json properties = nlohmann::json::object();
        properties["topic"] = topic;
        properties["frame_id"] = msg.header.frame_id;
        properties["color"] = nlohmann::json::array({ color.r, color.g, color.b, color.a });
        properties["n"] = coords.size();

        if (coords.size() >= 2)
        {
            nlohmann::json line;
            line["type"] = "Feature";
            line["properties"] = properties;
            line["properties"]["kind"] = "geopath";
            line["geometry"] = { { "type", "LineString" }, { "coordinates", coords } };
            fc["features"].push_back(std::move(line));
        }

        if (coords.size() >= 1)
        {
            nlohmann::json start;
            start["type"] = "Feature";
            start["properties"] = properties;
            start["properties"]["kind"] = "start";
            start["geometry"] = { { "type", "Point" }, { "coordinates", coords.front() } };
            fc["features"].push_back(std::move(start));

            nlohmann::json goal;
            goal["type"] = "Feature";
            goal["properties"] = properties;
            goal["properties"]["kind"] = "goal";
            goal["geometry"] = { { "type", "Point" }, { "coordinates", coords.back() } };
            fc["features"].push_back(std::move(goal));
        }

        return fc.dump();
    }

    void onGeoPath(const geographic_msgs::msg::GeoPath& msg, const GeoPathSource::SharedPtr& source)
    {
        if (!source || !source->chan.has_value())
        {
            return;
        }

        const rclcpp::Time now = this->now();
        source->last_seen_ns.store(now.nanoseconds(), std::memory_order_relaxed);

        const rclcpp::Time stamp{ msg.header.stamp };
        const uint64_t log_time = (stamp.nanoseconds() > 0) ? static_cast<uint64_t>(stamp.nanoseconds()) : static_cast<uint64_t>(now.nanoseconds());

        foxglove::schemas::GeoJSON out{};
        out.geojson = geoPathToGeoJson(msg, source->topic, source->color);

        const foxglove::FoxgloveError result = source->chan->log(out, log_time);
        if (result != foxglove::FoxgloveError::Ok)
        {
            RCLCPP_WARN_THROTTLE(get_logger(), *this->get_clock(), 5000, "Failed to log GeoPath as GeoJSON on '%s': %s", source->foxglove_topic.c_str(),
                                 foxglove::strerror(result));
        }
    }

    void onSnapshot(const std::string& topic, const data_tamer_msgs::msg::Snapshot& msg)
    {
        std::shared_ptr<ChannelInfo> ch = registry_.get_mutable(msg.schema_hash);
        if (!ch)
        {
            RCLCPP_DEBUG_THROTTLE(get_logger(), *this->get_clock(), 2000, "No schema yet for hash=%lu (topic=%s)", (unsigned long)msg.schema_hash, topic.c_str());
            return;
        }

        // Take a local copy to avoid races with the sweeper resetting ch->channel
        auto chan = ch->channel;
        if (use_protobuf_)
        {
            if (!chan)
            {
                foxglove::Schema sch{};
                sch.encoding = "protobuf";
                sch.name = ch->proto_runtime.full_type;
                sch.data = reinterpret_cast<const std::byte*>(ch->proto_runtime.fdset_bytes.data());
                sch.data_len = ch->proto_runtime.fdset_bytes.size();
                foxglove::FoxgloveResult<foxglove::RawChannel> r = foxglove::RawChannel::create(ch->topic, "protobuf", std::move(sch), context_);
                if (!r.has_value())
                {
                    RCLCPP_WARN(get_logger(), "Recreate proto chan failed for '%s'", ch->topic.c_str());
                    return;
                }
                ch->channel = std::make_shared<foxglove::RawChannel>(std::move(r.value()));
                chan = ch->channel;
            }

            const google::protobuf::Message* proto = ch->proto_runtime.factory->GetPrototype(ch->proto_runtime.desc);
            if (!proto)
            {
                RCLCPP_WARN(get_logger(), "No prototype for %s", ch->proto_runtime.full_type.c_str());
                return;
            }

            if (!ch->m)
            {
                ch->m.reset(proto->New());  // allocate once
            }
            std::lock_guard<std::mutex> lk(ch->pb_mutex);
            google::protobuf::Message* m = ch->m.get();
            m->Clear();  // reuse memory

            DataTamerParser::SnapshotView sv;
            sv.schema_hash = msg.schema_hash;
            sv.timestamp = msg.timestamp_nsec;
            sv.active_mask = { msg.active_mask.data(), msg.active_mask.size() };
            sv.payload = { msg.payload.data(), msg.payload.size() };

            thread_local std::string bytes;
            if (!data_tamer_tools::encodeSnapshot(ch->parsed_schema, sv, ch->proto_runtime, bytes, m))
            {
                RCLCPP_ERROR(get_logger(), "Failed to encode snapshot");
                return;
            }

            ch->channel->log(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size(), msg.timestamp_nsec);
        }
        else
        {
            // Re-create Foxglove channel lazily if soft-evicted
            if (!chan)
            {
                foxglove::Schema s{};
                s.encoding = "jsonschema";
                s.data = reinterpret_cast<const std::byte*>(ch->json_schema.data());
                s.data_len = ch->json_schema.size();

                const std::string resolved_topic = ch->topic;

                foxglove::FoxgloveResult<foxglove::RawChannel> r = foxglove::RawChannel::create(resolved_topic, "json", std::move(s), context_);
                if (!r.has_value())
                {
                    RCLCPP_WARN(get_logger(), "Re-create channel failed for '%s'", resolved_topic.c_str());
                    return;
                }
                ch->channel = std::make_shared<foxglove::RawChannel>(std::move(r.value()));
                chan = ch->channel;  // refresh local copy
                RCLCPP_INFO(get_logger(), "Recreated Foxglove channel '%s'", resolved_topic.c_str());
            }

            std::string json_text = data_tamer_tools::serializeSnapshotToJson(ch->parsed_schema, msg.active_mask, msg.payload,
                                                                              std::chrono::nanoseconds{ static_cast<int64_t>(msg.timestamp_nsec) })
                                        .dump();

            chan->log(reinterpret_cast<const std::byte*>(json_text.data()), json_text.size(), msg.timestamp_nsec);
        }

        registry_.touch(msg.schema_hash, this->now());
    }

    std::optional<foxglove::WebSocketServer> server_;
    foxglove::Context context_;
    bool enable_mcap_{ false };
    std::optional<foxglove::McapWriter> mcap_writer_;
    std::mutex mcap_mtx_;
    std::string mcap_path_;
    std::string mcap_dir_{ "." };
    std::string mcap_filename_{ "foxglove_relay.mcap" };
    std::string mcap_profile_{ "protobuf" };
    bool mcap_truncate_{ false };
    bool mcap_use_chunks_{ true };
    uint64_t mcap_chunk_size_{ 0 };
    foxglove::McapCompression mcap_compression_{ foxglove::McapCompression::Zstd };
    bool mcap_append_timestamp_{ true };
    std::string rotate_dir_topic_{};
    rclcpp::Subscription<data_tamer_tools::msg::LogDir>::SharedPtr rotate_sub_;
    rclcpp::TimerBase::SharedPtr rotate_discovery_timer_;
    std::string location_fix_prefix_{ "/locations" };
    std::string geojson_prefix_{ "/geojson" };
    rclcpp::TimerBase::SharedPtr discovery_timer_;

    std::unordered_map<std::string, rclcpp::Subscription<data_tamer_msgs::msg::Snapshot>::SharedPtr> snapshot_subs_;
    std::unordered_map<std::string, rclcpp::Subscription<data_tamer_msgs::msg::Schema>::SharedPtr> schema_subs_;
    std::unordered_map<std::string, rclcpp::Subscription<data_tamer_msgs::msg::Schemas>::SharedPtr> schemas_subs_;

    SchemaRegistry registry_;
    NavSatFixRegistry nsf_registry_;
    GeoPathRegistry geopath_registry_;

    rclcpp::TimerBase::SharedPtr eviction_timer_;
    rclcpp::Duration eviction_ttl_{ 0, 0 };
    rclcpp::Duration navsat_ttl_{ 0, 0 };
    rclcpp::Duration geopath_ttl_{ 0, 0 };
    rclcpp::QoS navsat_qos_{ rclcpp::SensorDataQoS() };

    std::optional<foxglove::schemas::LogChannel> rosout_chan_;
    rclcpp::Subscription<rcl_interfaces::msg::Log>::SharedPtr rosout_sub_;

    rclcpp::CallbackGroup::SharedPtr snapshot_callback_group_;
    rclcpp::CallbackGroup::SharedPtr rosout_callback_group_;
    rclcpp::CallbackGroup::SharedPtr timer_callback_group_;
    rclcpp::CallbackGroup::SharedPtr navsat_callback_group_;
    rclcpp::CallbackGroup::SharedPtr geopath_callback_group_;
    rclcpp::CallbackGroup::SharedPtr mcap_callback_group_;

    bool use_protobuf_{ false };

    void evictStalePublishers()
    {
        rclcpp::Time t = now();
        const size_t closed = registry_.soft_sweep_idle_channels(t, eviction_ttl_);
        if (closed > 0)
        {
            RCLCPP_INFO(get_logger(), "GC: soft-evicted (closed) %zu Foxglove channels idle > %.0f s; registry size: %zu", closed, eviction_ttl_.seconds(),
                        registry_.size());
        }

        if (navsat_ttl_.nanoseconds() > 0)
        {
            const size_t dropped_sources = nsf_registry_.prune(t, navsat_ttl_);
            if (dropped_sources > 0)
            {
                RCLCPP_INFO(get_logger(), "GC: pruned %zu stale NavSatFix sources idle > %.0f s", dropped_sources, navsat_ttl_.seconds());
            }
        }

        if (geopath_ttl_.nanoseconds() > 0)
        {
            const size_t dropped_sources = geopath_registry_.prune(t.nanoseconds(), geopath_ttl_.nanoseconds());
            if (dropped_sources > 0)
            {
                RCLCPP_INFO(get_logger(), "GC: pruned %zu stale GeoPath sources idle > %.0f s", dropped_sources, geopath_ttl_.seconds());
            }
        }
    }

    void onRosout(const rcl_interfaces::msg::Log& m)
    {
        if (!rosout_chan_.has_value())
        {
            return;  // not enabled / failed to init
        }

        // Build foxglove.schemas.Log
        foxglove::schemas::Log ev{};
        ev.timestamp = foxglove::schemas::Timestamp{ static_cast<uint32_t>(m.stamp.sec), static_cast<uint32_t>(m.stamp.nanosec) };
        ev.level = [&]
        {
            switch (m.level)
            {
                case rcl_interfaces::msg::Log::DEBUG:
                    return foxglove::schemas::Log::LogLevel::DEBUG;
                case rcl_interfaces::msg::Log::INFO:
                    return foxglove::schemas::Log::LogLevel::INFO;
                case rcl_interfaces::msg::Log::WARN:
                    return foxglove::schemas::Log::LogLevel::WARNING;
                case rcl_interfaces::msg::Log::ERROR:
                    return foxglove::schemas::Log::LogLevel::ERROR;
                case rcl_interfaces::msg::Log::FATAL:
                    return foxglove::schemas::Log::LogLevel::FATAL;
                default:
                    return foxglove::schemas::Log::LogLevel::UNKNOWN;
            }
        }();
        ev.message = m.msg;
        ev.name = m.name;  // node name
        ev.file = m.file;
        ev.line = static_cast<uint32_t>(m.line);

        (void)rosout_chan_->log(ev, this->now().nanoseconds());
    }
};
}  // namespace data_tamer_tools
#include <rclcpp_components/register_node_macro.hpp>
RCLCPP_COMPONENTS_REGISTER_NODE(data_tamer_tools::DtRos2ToFoxgloveBridge)
