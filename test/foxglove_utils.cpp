#include <data_tamer_tools/foxglove_utils.hpp>

#include <gtest/gtest.h>
#include <rmw/types.h>
#include <sensor_msgs/msg/laser_scan.hpp>
#include <visualization_msgs/msg/marker.hpp>

namespace
{
TEST(FoxgloveUtilsTest, JoinTopicPrefixNormalizesSlashes)
{
    EXPECT_EQ(data_tamer_tools::joinTopicPrefix("/scan", "/oa_sonar"), "/scan/oa_sonar");
    EXPECT_EQ(data_tamer_tools::joinTopicPrefix("/scan/", "/oa_sonar"), "/scan/oa_sonar");
    EXPECT_EQ(data_tamer_tools::joinTopicPrefix("/scan", "oa_sonar"), "/scan/oa_sonar");
    EXPECT_EQ(data_tamer_tools::joinTopicPrefix("", "/oa_sonar"), "/oa_sonar");
}

TEST(FoxgloveUtilsTest, NavSatQoSMapsKnownProfiles)
{
    const auto sensor_profile = data_tamer_tools::navsatQoS("sensor").get_rmw_qos_profile();
    EXPECT_EQ(sensor_profile.reliability, RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT);

    const auto reliable_profile = data_tamer_tools::navsatQoS("reliable").get_rmw_qos_profile();
    EXPECT_EQ(reliable_profile.reliability, RMW_QOS_POLICY_RELIABILITY_RELIABLE);

    const auto best_effort_profile = data_tamer_tools::navsatQoS("best_effort").get_rmw_qos_profile();
    EXPECT_EQ(best_effort_profile.reliability, RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT);
}

TEST(FoxgloveUtilsTest, TopicColorIsDeterministic)
{
    const auto color_a = data_tamer_tools::topicColor("/scan/oa_sonar");
    const auto color_b = data_tamer_tools::topicColor("/scan/oa_sonar");
    const auto color_c = data_tamer_tools::topicColor("/scan/other");

    EXPECT_DOUBLE_EQ(color_a.r, color_b.r);
    EXPECT_DOUBLE_EQ(color_a.g, color_b.g);
    EXPECT_DOUBLE_EQ(color_a.b, color_b.b);
    EXPECT_DOUBLE_EQ(color_a.a, 1.0);
    EXPECT_NE(color_a.r + color_a.g + color_a.b, color_c.r + color_c.g + color_c.b);
}

TEST(FoxgloveUtilsTest, ToFoxgloveLaserScanUsesFallbackFrameAndCopiesArrays)
{
    sensor_msgs::msg::LaserScan scan;
    scan.header.frame_id = "";
    scan.angle_min = -1.0f;
    scan.angle_max = 1.0f;
    scan.ranges = { 1.5f, 2.5f };
    scan.intensities = { 0.1f, 0.2f };

    const auto foxglove_scan = data_tamer_tools::toFoxgloveLaserScan(scan, "fallback_frame");

    EXPECT_EQ(foxglove_scan.frame_id, "fallback_frame");
    EXPECT_DOUBLE_EQ(foxglove_scan.start_angle, -1.0);
    EXPECT_DOUBLE_EQ(foxglove_scan.end_angle, 1.0);
    ASSERT_EQ(foxglove_scan.ranges.size(), 2u);
    EXPECT_DOUBLE_EQ(foxglove_scan.ranges[0], 1.5);
    EXPECT_DOUBLE_EQ(foxglove_scan.ranges[1], 2.5);
    ASSERT_EQ(foxglove_scan.intensities.size(), 2u);
    EXPECT_NEAR(foxglove_scan.intensities[0], 0.1, 1e-6);
    EXPECT_NEAR(foxglove_scan.intensities[1], 0.2, 1e-6);
}

TEST(FoxgloveUtilsTest, MarkerEntityIdUsesNamespaceWhenPresent)
{
    visualization_msgs::msg::Marker marker;
    marker.id = 7;
    marker.ns = "oa";
    EXPECT_EQ(data_tamer_tools::markerEntityId(marker), "oa:7");

    marker.ns.clear();
    EXPECT_EQ(data_tamer_tools::markerEntityId(marker), "7");
}
}  // namespace
