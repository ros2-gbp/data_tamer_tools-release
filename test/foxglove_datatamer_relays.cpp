#include <ament_index_cpp/get_package_share_directory.hpp>
#include <data_tamer_msgs/msg/schema.hpp>
#include <data_tamer_msgs/msg/snapshot.hpp>
#include <data_tamer_parser/data_tamer_parser.hpp>
#include <data_tamer_tools/foxglove_datatamer_relays.hpp>
#include <foxglove/context.hpp>
#include <gtest/gtest.h>
#include <rclcpp/executors/single_threaded_executor.hpp>
#include <rclcpp/rclcpp.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace
{
#pragma pack(push, 1)
struct BinHeader
{
    uint64_t schema_hash;
    uint64_t timestamp_ns;
    uint32_t active_len;
    uint32_t payload_len;
};
#pragma pack(pop)

struct SampleSnapshotData
{
    std::string schema_text;
    data_tamer_msgs::msg::Schema schema_msg;
    data_tamer_msgs::msg::Snapshot snapshot_msg;
};

SampleSnapshotData loadSampleSnapshotData()
{
    const std::string base = ament_index_cpp::get_package_share_directory("data_tamer_tools") + "/test/data";

    std::ifstream schema_file(base + "/capture.3026812782465800491.schema.txt", std::ios::binary);
    EXPECT_TRUE(schema_file.good());
    std::string schema_text(std::istreambuf_iterator<char>(schema_file), {});

    std::ifstream snapshot_file(base + "/capture.3026812782465800491.snapshot.bin", std::ios::binary);
    EXPECT_TRUE(snapshot_file.good());

    BinHeader header{};
    snapshot_file.read(reinterpret_cast<char*>(&header), sizeof(header));
    EXPECT_TRUE(snapshot_file.good());

    std::vector<uint8_t> active_mask(header.active_len);
    std::vector<uint8_t> payload(header.payload_len);
    snapshot_file.read(reinterpret_cast<char*>(active_mask.data()), static_cast<std::streamsize>(active_mask.size()));
    snapshot_file.read(reinterpret_cast<char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    EXPECT_TRUE(snapshot_file.good());

    const auto parsed_schema = DataTamerParser::BuilSchemaFromText(schema_text);

    SampleSnapshotData data;
    data.schema_text = std::move(schema_text);
    data.schema_msg.hash = header.schema_hash;
    data.schema_msg.channel_name = parsed_schema.channel_name;
    data.schema_msg.schema_text = data.schema_text;

    data.snapshot_msg.schema_hash = header.schema_hash;
    data.snapshot_msg.timestamp_nsec = header.timestamp_ns;
    data.snapshot_msg.active_mask = std::move(active_mask);
    data.snapshot_msg.payload = std::move(payload);
    return data;
}

class RclcppFixture : public ::testing::Test
{
  protected:
    static void SetUpTestSuite()
    {
        if (!rclcpp::ok())
        {
            int argc = 0;
            char** argv = nullptr;
            rclcpp::init(argc, argv);
        }
    }

    static void TearDownTestSuite()
    {
        if (rclcpp::ok())
        {
            rclcpp::shutdown();
        }
    }
};

class DataTamerRelayManagerFixture : public RclcppFixture
{
  protected:
    void SetUp() override
    {
        relay_node_ = std::make_shared<rclcpp::Node>("datatamer_relay_manager_test_node");
        publisher_node_ = std::make_shared<rclcpp::Node>("datatamer_relay_manager_pub_node");
        callback_group_ = relay_node_->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);
        context_ = foxglove::Context::create();
        manager_ = std::make_unique<data_tamer_tools::DataTamerRelayManager>(*relay_node_, context_, data_tamer_tools::DataTamerRelayOptions{ use_protobuf_ },
                                                                             data_tamer_tools::DataTamerRelayCallbackGroups{ callback_group_ });

        executor_.add_node(relay_node_);
        executor_.add_node(publisher_node_);
    }

    void TearDown() override
    {
        manager_.reset();
        executor_.remove_node(relay_node_);
        executor_.remove_node(publisher_node_);
        relay_node_.reset();
        publisher_node_.reset();
        callback_group_.reset();
    }

    bool spinUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout = std::chrono::milliseconds(2000))
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            executor_.spin_some();
            if (predicate())
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        executor_.spin_some();
        return predicate();
    }

    size_t closeAllWithFutureTtl()
    {
        const auto ttl = rclcpp::Duration::from_seconds(1);
        const auto future_time = relay_node_->now() + ttl + rclcpp::Duration::from_nanoseconds(1);
        return manager_->softSweepIdleChannels(future_time, ttl);
    }

    bool use_protobuf_{ false };
    foxglove::Context context_;
    rclcpp::executors::SingleThreadedExecutor executor_;
    rclcpp::CallbackGroup::SharedPtr callback_group_;
    std::shared_ptr<rclcpp::Node> relay_node_;
    std::shared_ptr<rclcpp::Node> publisher_node_;
    std::unique_ptr<data_tamer_tools::DataTamerRelayManager> manager_;
};

TEST_F(DataTamerRelayManagerFixture, EnsureSourceForTypeRoutesKnownTypes)
{
    EXPECT_TRUE(manager_->ensureSourceForType("data_tamer_msgs/msg/Snapshot", "/test/snapshot"));
    EXPECT_TRUE(manager_->ensureSourceForType("data_tamer_msgs/msg/Schema", "/test/schema"));
    EXPECT_TRUE(manager_->ensureSourceForType("data_tamer_msgs/msg/Schemas", "/test/schemas"));
    EXPECT_FALSE(manager_->ensureSourceForType("std_msgs/msg/String", "/test/other"));
}

TEST_F(DataTamerRelayManagerFixture, JsonLifecycleRecreatesClosedChannelOnSnapshot)
{
    const auto sample = loadSampleSnapshotData();

    ASSERT_TRUE(manager_->ensureSourceForType("data_tamer_msgs/msg/Schema", "/test/schema"));
    ASSERT_TRUE(manager_->ensureSourceForType("data_tamer_msgs/msg/Snapshot", "/test/snapshot"));

    auto schema_pub = publisher_node_->create_publisher<data_tamer_msgs::msg::Schema>("/test/schema", rclcpp::QoS(rclcpp::KeepLast(10)).transient_local().reliable());
    auto snapshot_pub = publisher_node_->create_publisher<data_tamer_msgs::msg::Snapshot>("/test/snapshot", rclcpp::QoS(rclcpp::KeepLast(10)).reliable());

    schema_pub->publish(sample.schema_msg);
    ASSERT_TRUE(spinUntil([this]() { return manager_->registrySize() == 1u; }));
    EXPECT_EQ(manager_->registrySize(), 1u);

    EXPECT_EQ(closeAllWithFutureTtl(), 1u);

    snapshot_pub->publish(sample.snapshot_msg);
    ASSERT_TRUE(spinUntil([this]() { return closeAllWithFutureTtl() == 1u; }));
}

TEST_F(DataTamerRelayManagerFixture, ProtobufLifecycleRecreatesClosedChannelOnSnapshot)
{
    use_protobuf_ = true;
    manager_ = std::make_unique<data_tamer_tools::DataTamerRelayManager>(*relay_node_, context_, data_tamer_tools::DataTamerRelayOptions{ use_protobuf_ },
                                                                         data_tamer_tools::DataTamerRelayCallbackGroups{ callback_group_ });

    const auto sample = loadSampleSnapshotData();

    ASSERT_TRUE(manager_->ensureSourceForType("data_tamer_msgs/msg/Schema", "/test/schema_proto"));
    ASSERT_TRUE(manager_->ensureSourceForType("data_tamer_msgs/msg/Snapshot", "/test/snapshot_proto"));

    auto schema_pub =
        publisher_node_->create_publisher<data_tamer_msgs::msg::Schema>("/test/schema_proto", rclcpp::QoS(rclcpp::KeepLast(10)).transient_local().reliable());
    auto snapshot_pub = publisher_node_->create_publisher<data_tamer_msgs::msg::Snapshot>("/test/snapshot_proto", rclcpp::QoS(rclcpp::KeepLast(10)).reliable());

    schema_pub->publish(sample.schema_msg);
    ASSERT_TRUE(spinUntil([this]() { return manager_->registrySize() == 1u; }));
    EXPECT_EQ(closeAllWithFutureTtl(), 1u);

    snapshot_pub->publish(sample.snapshot_msg);
    ASSERT_TRUE(spinUntil([this]() { return closeAllWithFutureTtl() == 1u; }));
}
}  // namespace
