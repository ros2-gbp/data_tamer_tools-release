#include <data_tamer_tools/foxglove_datatamer_relays.hpp>

#include <data_tamer_msgs/msg/schema.hpp>
#include <data_tamer_msgs/msg/schemas.hpp>
#include <data_tamer_msgs/msg/snapshot.hpp>
#include <data_tamer_parser/data_tamer_parser.hpp>
#include <data_tamer_tools/helpers.hpp>

#include <foxglove/channel.hpp>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <rclcpp/rclcpp.hpp>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

namespace data_tamer_tools
{
namespace
{
struct ChannelInfo
{
    std::string topic;
    std::string json_schema;
    DataTamerParser::Schema parsed_schema;
    std::shared_ptr<foxglove::RawChannel> channel;
    rclcpp::Time last_seen{ 0, 0 };
    ProtoRuntime proto_runtime;

    std::mutex pb_mutex;
    std::unique_ptr<google::protobuf::Message> message;
};

class SchemaRegistry
{
  public:
    void touch(uint64_t hash, const rclcpp::Time& now)
    {
        std::scoped_lock lk(mutex_);
        auto it = map_.find(hash);
        if (it != map_.end() && it->second)
        {
            it->second->last_seen = now;
        }
    }

    std::shared_ptr<ChannelInfo> getMutable(uint64_t hash)
    {
        std::scoped_lock lk(mutex_);
        auto it = map_.find(hash);
        return (it == map_.end()) ? nullptr : it->second;
    }

    size_t softSweepIdleChannels(const rclcpp::Time& now, const rclcpp::Duration& ttl)
    {
        std::scoped_lock lk(mutex_);
        size_t closed = 0;
        for (auto& [hash, info] : map_)
        {
            (void)hash;
            if (!info || info->last_seen.nanoseconds() == 0)
            {
                continue;
            }
            if ((now - info->last_seen) <= ttl)
            {
                continue;
            }
            if (info->channel)
            {
                info->channel.reset();
                ++closed;
            }
        }
        return closed;
    }

    size_t size() const
    {
        std::scoped_lock lk(mutex_);
        return map_.size();
    }

    bool has(uint64_t hash) const
    {
        std::scoped_lock lk(mutex_);
        return map_.count(hash) != 0;
    }

    bool tryEmplace(uint64_t hash, std::shared_ptr<ChannelInfo> info)
    {
        std::scoped_lock lk(mutex_);
        auto [it, inserted] = map_.emplace(hash, std::move(info));
        (void)it;
        return inserted;
    }

  private:
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<ChannelInfo>> map_;
};
}  // namespace

class DataTamerRelayManager::Impl
{
  public:
    Impl(rclcpp::Node& node, foxglove::Context& context, DataTamerRelayOptions options, DataTamerRelayCallbackGroups callback_groups)
      : node_(node), context_(context), options_(options), callback_groups_(callback_groups)
    {
    }

    bool ensureSourceForType(std::string_view ros_type, const std::string& topic)
    {
        if (ros_type == "data_tamer_msgs/msg/Snapshot")
        {
            ensureSnapshotSource(topic);
            return true;
        }
        if (ros_type == "data_tamer_msgs/msg/Schema")
        {
            ensureSchemaSource(topic);
            return true;
        }
        if (ros_type == "data_tamer_msgs/msg/Schemas")
        {
            ensureSchemasSource(topic);
            return true;
        }
        return false;
    }

    size_t softSweepIdleChannels(const rclcpp::Time& now, const rclcpp::Duration& ttl)
    {
        return registry_.softSweepIdleChannels(now, ttl);
    }

    size_t registrySize() const
    {
        return registry_.size();
    }

  private:
    void ensureSnapshotSource(const std::string& topic)
    {
        if (snapshot_subs_.count(topic) != 0)
        {
            return;
        }

        rclcpp::SubscriptionOptions snapshot_options;
        snapshot_options.callback_group = callback_groups_.snapshot_callback_group;
        auto qos = rclcpp::QoS(rclcpp::KeepLast(100)).reliable();
        auto sub = node_.create_subscription<data_tamer_msgs::msg::Snapshot>(
            topic, qos, [this, topic](data_tamer_msgs::msg::Snapshot::ConstSharedPtr msg) { this->onSnapshot(topic, *msg); }, snapshot_options);
        snapshot_subs_.emplace(topic, sub);
        RCLCPP_INFO(node_.get_logger(), "Subscribed to Snapshot: %s", topic.c_str());
    }

    void ensureSchemaSource(const std::string& topic)
    {
        if (schema_subs_.count(topic) != 0)
        {
            return;
        }

        rclcpp::SubscriptionOptions snapshot_options;
        snapshot_options.callback_group = callback_groups_.snapshot_callback_group;
        auto qos = rclcpp::QoS(rclcpp::KeepLast(10)).transient_local().reliable();
        auto sub = node_.create_subscription<data_tamer_msgs::msg::Schema>(
            topic, qos, [this](data_tamer_msgs::msg::Schema::ConstSharedPtr msg) { this->onSchema(*msg); }, snapshot_options);
        schema_subs_.emplace(topic, sub);
        RCLCPP_INFO(node_.get_logger(), "Subscribed to Schema: %s", topic.c_str());
    }

    void ensureSchemasSource(const std::string& topic)
    {
        if (schemas_subs_.count(topic) != 0)
        {
            return;
        }

        rclcpp::SubscriptionOptions snapshot_options;
        snapshot_options.callback_group = callback_groups_.snapshot_callback_group;
        auto qos = rclcpp::QoS(rclcpp::KeepLast(10)).transient_local().reliable();
        auto sub = node_.create_subscription<data_tamer_msgs::msg::Schemas>(
            topic, qos, [this](data_tamer_msgs::msg::Schemas::ConstSharedPtr msg) { this->onSchemas(*msg); }, snapshot_options);
        schemas_subs_.emplace(topic, sub);
        RCLCPP_INFO(node_.get_logger(), "Subscribed to Schemas: %s", topic.c_str());
    }

    void onSchemas(const data_tamer_msgs::msg::Schemas& msg)
    {
        for (const auto& schema : msg.schemas)
        {
            onSchema(schema);
        }
    }

    void onSchema(const data_tamer_msgs::msg::Schema& msg)
    {
        if (registry_.has(msg.hash))
        {
            registry_.touch(msg.hash, node_.now());
            RCLCPP_INFO(node_.get_logger(), "Schema already registered for hash=%lu", static_cast<unsigned long>(msg.hash));
            return;
        }

        std::shared_ptr<ChannelInfo> info = std::make_shared<ChannelInfo>();
        info->parsed_schema = DataTamerParser::BuilSchemaFromText(msg.schema_text);
        info->topic = msg.channel_name;

        foxglove::Schema schema{};
        std::string message_encoding;
        if (options_.use_protobuf)
        {
            info->proto_runtime = buildProto(info->parsed_schema);

            schema.encoding = "protobuf";
            schema.name = info->proto_runtime.full_type;
            schema.data = reinterpret_cast<const std::byte*>(info->proto_runtime.fdset_bytes.data());
            schema.data_len = info->proto_runtime.fdset_bytes.size();
            message_encoding = "protobuf";
        }
        else
        {
            info->json_schema = convertToJSONSchema(info->parsed_schema).dump();
            schema.encoding = "jsonschema";
            schema.data = reinterpret_cast<const std::byte*>(info->json_schema.data());
            schema.data_len = info->json_schema.size();
            message_encoding = "json";
        }

        auto channel = foxglove::RawChannel::create(info->topic, message_encoding, std::move(schema), context_);
        if (!channel.has_value())
        {
            RCLCPP_ERROR(node_.get_logger(), "Failed to create channel '%s'", info->topic.c_str());
            return;
        }
        info->channel = std::make_shared<foxglove::RawChannel>(std::move(channel.value()));
        info->last_seen = node_.now();
        if (!registry_.tryEmplace(msg.hash, info))
        {
            return;
        }

        RCLCPP_INFO(node_.get_logger(), "Registered Foxglove channel '%s' (hash=%lu)", msg.channel_name.c_str(), static_cast<unsigned long>(msg.hash));
    }

    void onSnapshot(const std::string& topic, const data_tamer_msgs::msg::Snapshot& msg)
    {
        std::shared_ptr<ChannelInfo> channel_info = registry_.getMutable(msg.schema_hash);
        if (!channel_info)
        {
            RCLCPP_DEBUG_THROTTLE(node_.get_logger(), *node_.get_clock(), 2000, "No schema yet for hash=%lu (topic=%s)", static_cast<unsigned long>(msg.schema_hash),
                                  topic.c_str());
            return;
        }

        auto channel = channel_info->channel;
        if (options_.use_protobuf)
        {
            if (!channel)
            {
                foxglove::Schema schema{};
                schema.encoding = "protobuf";
                schema.name = channel_info->proto_runtime.full_type;
                schema.data = reinterpret_cast<const std::byte*>(channel_info->proto_runtime.fdset_bytes.data());
                schema.data_len = channel_info->proto_runtime.fdset_bytes.size();
                auto recreated = foxglove::RawChannel::create(channel_info->topic, "protobuf", std::move(schema), context_);
                if (!recreated.has_value())
                {
                    RCLCPP_WARN(node_.get_logger(), "Recreate proto chan failed for '%s'", channel_info->topic.c_str());
                    return;
                }
                channel_info->channel = std::make_shared<foxglove::RawChannel>(std::move(recreated.value()));
                channel = channel_info->channel;
            }

            const google::protobuf::Message* prototype = channel_info->proto_runtime.factory->GetPrototype(channel_info->proto_runtime.desc);
            if (!prototype)
            {
                RCLCPP_WARN(node_.get_logger(), "No prototype for %s", channel_info->proto_runtime.full_type.c_str());
                return;
            }

            if (!channel_info->message)
            {
                channel_info->message.reset(prototype->New());
            }

            std::lock_guard<std::mutex> lk(channel_info->pb_mutex);
            google::protobuf::Message* scratch = channel_info->message.get();
            scratch->Clear();

            DataTamerParser::SnapshotView snapshot_view;
            snapshot_view.schema_hash = msg.schema_hash;
            snapshot_view.timestamp = msg.timestamp_nsec;
            snapshot_view.active_mask = { msg.active_mask.data(), msg.active_mask.size() };
            snapshot_view.payload = { msg.payload.data(), msg.payload.size() };

            thread_local std::string bytes;
            if (!encodeSnapshot(channel_info->parsed_schema, snapshot_view, channel_info->proto_runtime, bytes, scratch))
            {
                RCLCPP_ERROR(node_.get_logger(), "Failed to encode snapshot");
                return;
            }

            channel_info->channel->log(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size(), msg.timestamp_nsec);
        }
        else
        {
            if (!channel)
            {
                foxglove::Schema schema{};
                schema.encoding = "jsonschema";
                schema.data = reinterpret_cast<const std::byte*>(channel_info->json_schema.data());
                schema.data_len = channel_info->json_schema.size();

                auto recreated = foxglove::RawChannel::create(channel_info->topic, "json", std::move(schema), context_);
                if (!recreated.has_value())
                {
                    RCLCPP_WARN(node_.get_logger(), "Re-create channel failed for '%s'", channel_info->topic.c_str());
                    return;
                }
                channel_info->channel = std::make_shared<foxglove::RawChannel>(std::move(recreated.value()));
                channel = channel_info->channel;
                RCLCPP_INFO(node_.get_logger(), "Recreated Foxglove channel '%s'", channel_info->topic.c_str());
            }

            std::string json_text =
                serializeSnapshotToJson(channel_info->parsed_schema, msg.active_mask, msg.payload, std::chrono::nanoseconds{ static_cast<int64_t>(msg.timestamp_nsec) })
                    .dump();

            channel->log(reinterpret_cast<const std::byte*>(json_text.data()), json_text.size(), msg.timestamp_nsec);
        }

        registry_.touch(msg.schema_hash, node_.now());
    }

    rclcpp::Node& node_;
    foxglove::Context& context_;
    DataTamerRelayOptions options_;
    DataTamerRelayCallbackGroups callback_groups_;

    std::unordered_map<std::string, rclcpp::Subscription<data_tamer_msgs::msg::Snapshot>::SharedPtr> snapshot_subs_;
    std::unordered_map<std::string, rclcpp::Subscription<data_tamer_msgs::msg::Schema>::SharedPtr> schema_subs_;
    std::unordered_map<std::string, rclcpp::Subscription<data_tamer_msgs::msg::Schemas>::SharedPtr> schemas_subs_;
    SchemaRegistry registry_;
};

DataTamerRelayManager::DataTamerRelayManager(rclcpp::Node& node, foxglove::Context& context, DataTamerRelayOptions options, DataTamerRelayCallbackGroups callback_groups)
  : impl_(std::make_unique<Impl>(node, context, options, callback_groups))
{
}

DataTamerRelayManager::~DataTamerRelayManager() = default;
DataTamerRelayManager::DataTamerRelayManager(DataTamerRelayManager&&) noexcept = default;
DataTamerRelayManager& DataTamerRelayManager::operator=(DataTamerRelayManager&&) noexcept = default;

bool DataTamerRelayManager::ensureSourceForType(std::string_view ros_type, const std::string& topic)
{
    return impl_->ensureSourceForType(ros_type, topic);
}

size_t DataTamerRelayManager::softSweepIdleChannels(const rclcpp::Time& now, const rclcpp::Duration& ttl)
{
    return impl_->softSweepIdleChannels(now, ttl);
}

size_t DataTamerRelayManager::registrySize() const
{
    return impl_->registrySize();
}
}  // namespace data_tamer_tools
