#include <data_tamer_tools/foxglove_typed_relays.hpp>

#include <foxglove/schemas.hpp>
#include <geographic_msgs/msg/geo_path.hpp>
#include <geometry_msgs/msg/point.hpp>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <visualization_msgs/msg/marker.hpp>

#include <limits>

namespace
{
TEST(FoxgloveTypedRelaysTest, GeoPathToGeoJsonBuildsLineStartGoalAndFiltersInvalidCoordinates)
{
    geographic_msgs::msg::GeoPath path;
    path.header.frame_id = "map";

    geographic_msgs::msg::GeoPoseStamped start;
    start.pose.position.latitude = 47.61;
    start.pose.position.longitude = -122.33;
    path.poses.push_back(start);

    geographic_msgs::msg::GeoPoseStamped invalid;
    invalid.pose.position.latitude = std::numeric_limits<double>::infinity();
    invalid.pose.position.longitude = -10.0;
    path.poses.push_back(invalid);

    geographic_msgs::msg::GeoPoseStamped goal;
    goal.pose.position.latitude = 48.12;
    goal.pose.position.longitude = -123.45;
    path.poses.push_back(goal);

    foxglove::schemas::Color color{ 0.1, 0.2, 0.3, 0.4 };

    const auto geojson_text = data_tamer_tools::geoPathToGeoJson(path, "/geo/path", color);
    const auto geojson = nlohmann::json::parse(geojson_text);

    ASSERT_EQ(geojson.at("type"), "FeatureCollection");
    const auto& features = geojson.at("features");
    ASSERT_EQ(features.size(), 3u);

    const auto& line = features.at(0);
    EXPECT_EQ(line.at("geometry").at("type"), "LineString");
    ASSERT_EQ(line.at("geometry").at("coordinates").size(), 2u);
    EXPECT_DOUBLE_EQ(line.at("geometry").at("coordinates").at(0).at(0).get<double>(), -122.33);
    EXPECT_DOUBLE_EQ(line.at("geometry").at("coordinates").at(0).at(1).get<double>(), 47.61);
    EXPECT_DOUBLE_EQ(line.at("geometry").at("coordinates").at(1).at(0).get<double>(), -123.45);
    EXPECT_DOUBLE_EQ(line.at("geometry").at("coordinates").at(1).at(1).get<double>(), 48.12);
    EXPECT_EQ(line.at("properties").at("kind"), "geopath");
    EXPECT_EQ(line.at("properties").at("topic"), "/geo/path");
    EXPECT_EQ(line.at("properties").at("frame_id"), "map");
    EXPECT_EQ(line.at("properties").at("n"), 2u);
    ASSERT_EQ(line.at("properties").at("color").size(), 4u);
    EXPECT_DOUBLE_EQ(line.at("properties").at("color").at(0).get<double>(), 0.1);
    EXPECT_DOUBLE_EQ(line.at("properties").at("color").at(3).get<double>(), 0.4);

    const auto& start_feature = features.at(1);
    EXPECT_EQ(start_feature.at("properties").at("kind"), "start");
    EXPECT_DOUBLE_EQ(start_feature.at("geometry").at("coordinates").at(0).get<double>(), -122.33);
    EXPECT_DOUBLE_EQ(start_feature.at("geometry").at("coordinates").at(1).get<double>(), 47.61);

    const auto& goal_feature = features.at(2);
    EXPECT_EQ(goal_feature.at("properties").at("kind"), "goal");
    EXPECT_DOUBLE_EQ(goal_feature.at("geometry").at("coordinates").at(0).get<double>(), -123.45);
    EXPECT_DOUBLE_EQ(goal_feature.at("geometry").at("coordinates").at(1).get<double>(), 48.12);
}

TEST(FoxgloveTypedRelaysTest, SceneEntityFromMarkerBuildsArrowPrimitiveWithFallbackFrame)
{
    visualization_msgs::msg::Marker marker;
    marker.header.frame_id = "";
    marker.ns = "oa";
    marker.id = 7;
    marker.type = visualization_msgs::msg::Marker::ARROW;
    marker.scale.x = 0.8;
    marker.scale.y = 0.2;
    marker.scale.z = 0.4;
    marker.pose.position.x = 1.0;
    marker.pose.position.y = 2.0;
    marker.pose.position.z = 3.0;
    marker.pose.orientation.w = 1.0;
    marker.color.r = 0.9f;
    marker.color.g = 0.1f;
    marker.color.b = 0.2f;
    marker.color.a = 0.7f;
    marker.lifetime.sec = 2;
    marker.lifetime.nanosec = 5;
    marker.frame_locked = true;

    const auto entity = data_tamer_tools::sceneEntityFromMarker(marker, "fallback_frame");

    ASSERT_TRUE(entity.has_value());
    EXPECT_EQ(entity->frame_id, "fallback_frame");
    EXPECT_EQ(entity->id, "oa:7");
    ASSERT_TRUE(entity->lifetime.has_value());
    EXPECT_EQ(entity->lifetime->sec, 2);
    EXPECT_EQ(entity->lifetime->nsec, 5u);
    EXPECT_TRUE(entity->frame_locked);
    ASSERT_EQ(entity->arrows.size(), 1u);
    EXPECT_TRUE(entity->lines.empty());

    const auto& arrow = entity->arrows.front();
    ASSERT_TRUE(arrow.pose.has_value());
    ASSERT_TRUE(arrow.pose->position.has_value());
    EXPECT_DOUBLE_EQ(arrow.pose->position->x, 1.0);
    EXPECT_DOUBLE_EQ(arrow.pose->position->y, 2.0);
    EXPECT_DOUBLE_EQ(arrow.pose->position->z, 3.0);
    EXPECT_DOUBLE_EQ(arrow.shaft_length, 0.56);
    EXPECT_DOUBLE_EQ(arrow.shaft_diameter, 0.2);
    EXPECT_DOUBLE_EQ(arrow.head_length, 0.24);
    EXPECT_DOUBLE_EQ(arrow.head_diameter, 0.4);
    ASSERT_TRUE(arrow.color.has_value());
    EXPECT_NEAR(arrow.color->r, 0.9, 1e-6);
    EXPECT_NEAR(arrow.color->a, 0.7, 1e-6);
}

TEST(FoxgloveTypedRelaysTest, SceneEntityFromMarkerUsesLinePrimitiveForArrowPointList)
{
    visualization_msgs::msg::Marker marker;
    marker.header.frame_id = "oa_local";
    marker.ns = "oa";
    marker.id = 3;
    marker.type = visualization_msgs::msg::Marker::ARROW;
    marker.scale.x = 0.05;
    marker.color.g = 1.0f;

    geometry_msgs::msg::Point p0;
    p0.x = 1.0;
    p0.y = 2.0;
    geometry_msgs::msg::Point p1;
    p1.x = 3.0;
    p1.y = 4.0;
    marker.points = { p0, p1 };

    const auto entity = data_tamer_tools::sceneEntityFromMarker(marker, "unused_frame");

    ASSERT_TRUE(entity.has_value());
    EXPECT_EQ(entity->frame_id, "oa_local");
    EXPECT_TRUE(entity->arrows.empty());
    ASSERT_EQ(entity->lines.size(), 1u);
    const auto& line = entity->lines.front();
    EXPECT_EQ(line.type, foxglove::schemas::LinePrimitive::LineType::LINE_STRIP);
    EXPECT_DOUBLE_EQ(line.thickness, 0.05);
    ASSERT_EQ(line.points.size(), 2u);
    EXPECT_DOUBLE_EQ(line.points.at(0).x, 1.0);
    EXPECT_DOUBLE_EQ(line.points.at(0).y, 2.0);
    EXPECT_DOUBLE_EQ(line.points.at(1).x, 3.0);
    EXPECT_DOUBLE_EQ(line.points.at(1).y, 4.0);
}

TEST(FoxgloveTypedRelaysTest, SceneEntityFromMarkerAppliesPoseToSphereListPoints)
{
    visualization_msgs::msg::Marker marker;
    marker.header.frame_id = "dvl";
    marker.ns = "hits";
    marker.id = 9;
    marker.type = visualization_msgs::msg::Marker::SPHERE_LIST;
    marker.scale.x = 0.1;
    marker.scale.y = 0.1;
    marker.scale.z = 0.1;
    marker.pose.position.x = 1.0;
    marker.pose.position.y = 2.0;
    marker.pose.position.z = 3.0;
    marker.pose.orientation.z = 0.7071067811865476;
    marker.pose.orientation.w = 0.7071067811865476;

    geometry_msgs::msg::Point point;
    point.x = 1.0;
    marker.points.push_back(point);

    const auto entity = data_tamer_tools::sceneEntityFromMarker(marker, "unused_frame");

    ASSERT_TRUE(entity.has_value());
    ASSERT_EQ(entity->spheres.size(), 1u);
    const auto& sphere = entity->spheres.front();
    ASSERT_TRUE(sphere.pose.has_value());
    ASSERT_TRUE(sphere.pose->position.has_value());
    EXPECT_NEAR(sphere.pose->position->x, 1.0, 1e-9);
    EXPECT_NEAR(sphere.pose->position->y, 3.0, 1e-9);
    EXPECT_NEAR(sphere.pose->position->z, 3.0, 1e-9);
}

TEST(FoxgloveTypedRelaysTest, SceneEntityFromMarkerRejectsUnsupportedMarkerType)
{
    visualization_msgs::msg::Marker marker;
    marker.type = visualization_msgs::msg::Marker::MESH_RESOURCE;

    const auto entity = data_tamer_tools::sceneEntityFromMarker(marker, "default_frame");

    EXPECT_FALSE(entity.has_value());
}
}  // namespace
