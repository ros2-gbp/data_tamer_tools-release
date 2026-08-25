#include <rcl_interfaces/msg/log.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp/qos.hpp>

#include <data_tamer_tools/foxglove_datatamer_relays.hpp>
#include <data_tamer_tools/foxglove_typed_relays.hpp>
#include <data_tamer_tools/foxglove_utils.hpp>
#include <data_tamer_tools/msg/log_dir.hpp>
#include <data_tamer_tools/ros_topic_discovery.hpp>

#include <foxglove/foxglove.hpp>
#include <foxglove/mcap.hpp>
#include <foxglove/websocket.hpp>
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
#include <optional>
#include <string>
#include <vector>

namespace data_tamer_tools
{

class DtRos2ToFoxgloveBridge : public rclcpp::Node
{
  public:
    DtRos2ToFoxgloveBridge(const rclcpp::NodeOptions& o)
      : rclcpp::Node("foxglove_relay", o)
      , context_(foxglove::Context::create())
      , data_tamer_relay_manager_(*this, context_, DataTamerRelayOptions{ use_protobuf_ }, DataTamerRelayCallbackGroups{ snapshot_callback_group_ })
      , typed_relay_manager_(
            *this, context_, TypedRelayOptions{ location_fix_prefix_, geojson_prefix_, laser_scan_prefix_, scene_prefix_, transform_prefix_, navsat_qos_ },
            TypedRelayCallbackGroups{ navsat_callback_group_, geopath_callback_group_, laser_scan_callback_group_, marker_callback_group_, tf_callback_group_ },
            TypedRelayRegistries{ nsf_registry_, geopath_registry_, laser_scan_registry_, marker_registry_, tf_registry_ })
    {
        using std::chrono::milliseconds;

        snapshot_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);
        rosout_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);
        timer_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);
        navsat_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::Reentrant);
        geopath_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::Reentrant);
        laser_scan_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::Reentrant);
        marker_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::Reentrant);
        tf_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::Reentrant);
        mcap_callback_group_ = this->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);

        // Foxglove WebSocket server
        foxglove::WebSocketServerOptions opts;
        opts.context = context_;
        opts.host = declare_parameter<std::string>("host", "0.0.0.0");
        opts.callbacks.onSubscribe = [this](uint64_t channel_id, const foxglove::ClientMetadata& client_metadata)
        { typed_relay_manager_.replayLatchedGeoJson(channel_id, client_metadata.sink_id); };

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

        rcl_interfaces::msg::ParameterDescriptor laser_scan_ttl_description;
        laser_scan_ttl_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        laser_scan_ttl_description.name = "laser_scan_ttl_sec";
        laser_scan_ttl_description.description = "Maximum age for LaserScan sources relayed to Foxglove LaserScan (0 to disable)";
        laser_scan_ttl_description.integer_range.resize(1);
        laser_scan_ttl_description.integer_range[0].from_value = 0;
        laser_scan_ttl_description.integer_range[0].to_value = 86400;
        laser_scan_ttl_description.integer_range[0].step = 1;
        laser_scan_ttl_description.read_only = true;
        laser_scan_ttl_ = rclcpp::Duration::from_seconds(declare_parameter<int>("laser_scan_ttl_sec", 0, laser_scan_ttl_description));

        rcl_interfaces::msg::ParameterDescriptor scene_ttl_description;
        scene_ttl_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        scene_ttl_description.name = "scene_ttl_sec";
        scene_ttl_description.description = "Maximum age for MarkerArray sources relayed to Foxglove SceneUpdate (0 to disable)";
        scene_ttl_description.integer_range.resize(1);
        scene_ttl_description.integer_range[0].from_value = 0;
        scene_ttl_description.integer_range[0].to_value = 86400;
        scene_ttl_description.integer_range[0].step = 1;
        scene_ttl_description.read_only = true;
        scene_ttl_ = rclcpp::Duration::from_seconds(declare_parameter<int>("scene_ttl_sec", 0, scene_ttl_description));

        rcl_interfaces::msg::ParameterDescriptor tf_ttl_description;
        tf_ttl_description.type = rclcpp::ParameterType::PARAMETER_INTEGER;
        tf_ttl_description.name = "tf_ttl_sec";
        tf_ttl_description.description = "Maximum age for TFMessage sources relayed to Foxglove FrameTransforms (0 to disable)";
        tf_ttl_description.integer_range.resize(1);
        tf_ttl_description.integer_range[0].from_value = 0;
        tf_ttl_description.integer_range[0].to_value = 86400;
        tf_ttl_description.integer_range[0].step = 1;
        tf_ttl_description.read_only = true;
        tf_ttl_ = rclcpp::Duration::from_seconds(declare_parameter<int>("tf_ttl_sec", 0, tf_ttl_description));

        location_fix_prefix_ = declare_parameter<std::string>("location_fix_prefix", "/locations");
        geojson_prefix_ = declare_parameter<std::string>("geojson_prefix", "/geojson");
        laser_scan_prefix_ = declare_parameter<std::string>("laser_scan_prefix", "/scan");
        scene_prefix_ = declare_parameter<std::string>("scene_prefix", "/scene");
        transform_prefix_ = declare_parameter<std::string>("transform_prefix", "/transforms");

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
            auto chRes = foxglove::messages::LogChannel::create("/rosout", context_);
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
        if (server_.has_value())
        {
            const foxglove::FoxgloveError e = server_->stop();
            if (e != foxglove::FoxgloveError::Ok)
            {
                RCLCPP_WARN(get_logger(), "Failed to stop Foxglove WebSocket server: %s", foxglove::strerror(e));
            }
            server_.reset();
        }
        closeMcapWriter();
    }

  private:
    // (Re)subscribe to every topic whose type == data_tamer_msgs/msg/Snapshot.
    void discoverSnapshotTopics()
    {
        const std::map<std::string, std::vector<std::string>> topics = this->get_topic_names_and_types();
        for (const auto& [name, types] : topics)
        {
            for (const auto& t : types)
            {
                (void)data_tamer_relay_manager_.ensureSourceForType(t, name);
                (void)typed_relay_manager_.ensureSourceForType(t, name);
            }
        }
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
            return schema_name == "foxglove.LocationFix" || schema_name == "foxglove.GeoJSON" || schema_name == "foxglove.LaserScan" ||
                   schema_name == "foxglove.SceneUpdate" || schema_name == "foxglove.FrameTransform" || schema_name == "foxglove.FrameTransforms";
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
    std::string laser_scan_prefix_{ "/scan" };
    std::string scene_prefix_{ "/scene" };
    std::string transform_prefix_{ "/transforms" };
    rclcpp::TimerBase::SharedPtr discovery_timer_;

    NavSatFixRegistry nsf_registry_;
    GeoPathRegistry geopath_registry_;
    LaserScanRegistry laser_scan_registry_;
    MarkerArrayRegistry marker_registry_;
    TfMessageRegistry tf_registry_;

    rclcpp::TimerBase::SharedPtr eviction_timer_;
    rclcpp::Duration eviction_ttl_{ 0, 0 };
    rclcpp::Duration navsat_ttl_{ 0, 0 };
    rclcpp::Duration geopath_ttl_{ 0, 0 };
    rclcpp::Duration laser_scan_ttl_{ 0, 0 };
    rclcpp::Duration scene_ttl_{ 0, 0 };
    rclcpp::Duration tf_ttl_{ 0, 0 };
    rclcpp::QoS navsat_qos_{ rclcpp::SensorDataQoS() };

    std::optional<foxglove::messages::LogChannel> rosout_chan_;
    rclcpp::Subscription<rcl_interfaces::msg::Log>::SharedPtr rosout_sub_;

    rclcpp::CallbackGroup::SharedPtr snapshot_callback_group_;
    rclcpp::CallbackGroup::SharedPtr rosout_callback_group_;
    rclcpp::CallbackGroup::SharedPtr timer_callback_group_;
    rclcpp::CallbackGroup::SharedPtr navsat_callback_group_;
    rclcpp::CallbackGroup::SharedPtr geopath_callback_group_;
    rclcpp::CallbackGroup::SharedPtr laser_scan_callback_group_;
    rclcpp::CallbackGroup::SharedPtr marker_callback_group_;
    rclcpp::CallbackGroup::SharedPtr tf_callback_group_;
    rclcpp::CallbackGroup::SharedPtr mcap_callback_group_;

    bool use_protobuf_{ false };
    DataTamerRelayManager data_tamer_relay_manager_;
    TypedRelayManager typed_relay_manager_;

    void evictStalePublishers()
    {
        rclcpp::Time t = now();
        const size_t closed = data_tamer_relay_manager_.softSweepIdleChannels(t, eviction_ttl_);
        if (closed > 0)
        {
            RCLCPP_INFO(get_logger(), "GC: soft-evicted (closed) %zu Foxglove channels idle > %.0f s; registry size: %zu", closed, eviction_ttl_.seconds(),
                        data_tamer_relay_manager_.registrySize());
        }

        if (navsat_ttl_.nanoseconds() > 0)
        {
            const size_t dropped_sources = nsf_registry_.prune(t.nanoseconds(), navsat_ttl_.nanoseconds());
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

        if (laser_scan_ttl_.nanoseconds() > 0)
        {
            const size_t dropped_sources = laser_scan_registry_.prune(t.nanoseconds(), laser_scan_ttl_.nanoseconds());
            if (dropped_sources > 0)
            {
                RCLCPP_INFO(get_logger(), "GC: pruned %zu stale LaserScan sources idle > %.0f s", dropped_sources, laser_scan_ttl_.seconds());
            }
        }

        if (scene_ttl_.nanoseconds() > 0)
        {
            const size_t dropped_sources = marker_registry_.prune(t.nanoseconds(), scene_ttl_.nanoseconds());
            if (dropped_sources > 0)
            {
                RCLCPP_INFO(get_logger(), "GC: pruned %zu stale MarkerArray sources idle > %.0f s", dropped_sources, scene_ttl_.seconds());
            }
        }

        if (tf_ttl_.nanoseconds() > 0)
        {
            const size_t dropped_sources = tf_registry_.prune(t.nanoseconds(), tf_ttl_.nanoseconds());
            if (dropped_sources > 0)
            {
                RCLCPP_INFO(get_logger(), "GC: pruned %zu stale TFMessage sources idle > %.0f s", dropped_sources, tf_ttl_.seconds());
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
        foxglove::messages::Log ev{};
        ev.timestamp = foxglove::messages::Timestamp{ static_cast<uint32_t>(m.stamp.sec), static_cast<uint32_t>(m.stamp.nanosec) };
        ev.level = [&]
        {
            switch (m.level)
            {
                case rcl_interfaces::msg::Log::DEBUG:
                    return foxglove::messages::Log::LogLevel::DEBUG;
                case rcl_interfaces::msg::Log::INFO:
                    return foxglove::messages::Log::LogLevel::INFO;
                case rcl_interfaces::msg::Log::WARN:
                    return foxglove::messages::Log::LogLevel::WARNING;
                case rcl_interfaces::msg::Log::ERROR:
                    return foxglove::messages::Log::LogLevel::ERROR;
                case rcl_interfaces::msg::Log::FATAL:
                    return foxglove::messages::Log::LogLevel::FATAL;
                default:
                    return foxglove::messages::Log::LogLevel::UNKNOWN;
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
