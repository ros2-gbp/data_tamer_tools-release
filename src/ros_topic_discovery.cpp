#include <data_tamer_tools/ros_topic_discovery.hpp>

#include <algorithm>
#include <map>
#include <sstream>
#include <vector>

namespace data_tamer_tools
{
std::optional<std::string> discoverTopicByType(const rclcpp::Node& node, std::string_view ros_msg_type, const rclcpp::Logger& logger, std::string_view param_name)
{
    const std::map<std::string, std::vector<std::string>> topics = node.get_topic_names_and_types();
    const std::string type_str(ros_msg_type);

    std::vector<std::string> matches;
    matches.reserve(topics.size());
    for (const auto& [topic_name, types] : topics)
    {
        if (std::find(types.begin(), types.end(), type_str) != types.end())
        {
            matches.push_back(topic_name);
        }
    }

    if (matches.empty())
    {
        return std::nullopt;
    }

    std::sort(matches.begin(), matches.end());

    if (matches.size() > 1)
    {
        std::ostringstream oss;
        for (size_t i = 0; i < matches.size(); ++i)
        {
            if (i)
            {
                oss << ", ";
            }
            oss << matches[i];
        }

        if (!param_name.empty())
        {
            RCLCPP_WARN(logger, "Discovered multiple topics of type '%s': [%s]. Using '%s'. Override with parameter '%s'.", type_str.c_str(), oss.str().c_str(),
                        matches.front().c_str(), std::string(param_name).c_str());
        }
        else
        {
            RCLCPP_WARN(logger, "Discovered multiple topics of type '%s': [%s]. Using '%s'.", type_str.c_str(), oss.str().c_str(), matches.front().c_str());
        }
    }

    return matches.front();
}
}  // namespace data_tamer_tools
