#pragma once

#include <rclcpp/subscription.hpp>

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace data_tamer_tools
{
template <typename MsgT, typename ChannelT>
struct TimedRelaySource
{
    using MessageType = MsgT;
    using ChannelType = ChannelT;

    std::string topic;
    std::string foxglove_topic;
    typename rclcpp::Subscription<MsgT>::SharedPtr sub;
    std::optional<ChannelT> chan;
    std::atomic<int64_t> last_seen_ns{ 0 };
};

template <typename SourceT>
class TimedSourceRegistry
{
  public:
    using SourceSharedPtr = std::shared_ptr<SourceT>;

    bool has(const std::string& topic) const
    {
        std::scoped_lock lk(m_);
        typename std::vector<SourceSharedPtr>::const_iterator it =
            std::find_if(sources_.begin(), sources_.end(), [&topic](const SourceSharedPtr& source) { return source && source->topic == topic; });
        return it != sources_.end();
    }

    void emplace(SourceSharedPtr source)
    {
        std::scoped_lock lk(m_);
        sources_.push_back(std::move(source));
    }

    size_t prune(int64_t now_ns, int64_t ttl_ns)
    {
        if (ttl_ns <= 0)
        {
            return 0;
        }

        std::scoped_lock lk(m_);
        const size_t before = sources_.size();
        sources_.erase(std::remove_if(sources_.begin(), sources_.end(),
                                      [&](const SourceSharedPtr& source)
                                      {
                                          if (!source || !source->sub || !source->chan.has_value())
                                          {
                                              return true;
                                          }
                                          const int64_t last_seen_ns = source->last_seen_ns.load(std::memory_order_relaxed);
                                          if (last_seen_ns <= 0)
                                          {
                                              return false;
                                          }
                                          return (now_ns - last_seen_ns) > ttl_ns;
                                      }),
                       sources_.end());
        return before - sources_.size();
    }

    std::vector<SourceSharedPtr> snapshot() const
    {
        std::scoped_lock lk(m_);
        return sources_;
    }

  private:
    std::vector<SourceSharedPtr> sources_;
    mutable std::mutex m_;
};
}  // namespace data_tamer_tools
