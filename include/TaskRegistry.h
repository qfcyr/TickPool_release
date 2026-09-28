#pragma once
// TaskRegistry.h —— 定义侧注册表

#include <string>
#include <vector>
#include <stdexcept>
#include <cstdint>
#include <ankerl/unordered_dense.h>

class TaskRegistry {
public:
    using TaskID = uint32_t;
    static constexpr TaskID kInvalidTaskID = UINT32_MAX;

    // 注册新任务（Definition 阶段；TaskID = 注册顺序）。重名抛 invalid_argument。
    TaskID registerKey(const TaskKey& key) {
        if (hasTask(key))
            throw std::invalid_argument("defineTask: key \"" + key + "\" already defined");
        TaskID id = static_cast<TaskID>(idToKey_.size());
        idToKey_.push_back(key);
        nameIndex_.emplace(key, id);
        return id;
    }

    // ---------- 运行期只读（submit / 序列化 / 命令日志 / 错误消息按名解析） ----------
    const std::vector<TaskKey>& idToKey() const noexcept { return idToKey_; }
    const TaskKey& idToKeyAt(TaskID id) const { return idToKey_[id]; }
    size_t taskCount() const noexcept { return idToKey_.size(); }

    bool hasTask(const TaskKey& key) const noexcept {
        return nameIndex_.find(key) != nameIndex_.end();
    }
    // 名字→ID
    TaskID taskId(const TaskKey& key) const {
        auto it = nameIndex_.find(key);
        if (it == nameIndex_.end())
            throw std::out_of_range("TaskKey \"" + key + "\" not found");
        return it->second;
    }
    TaskID taskIdOrInvalid(const TaskKey& key) const noexcept {
        auto it = nameIndex_.find(key);
        return it == nameIndex_.end() ? kInvalidTaskID : it->second;
    }

private:
    std::vector<TaskKey> idToKey_;                                          // TaskID → TaskKey（注册顺序）
    ankerl::unordered_dense::map<TaskKey, TaskID, std::hash<std::string>> nameIndex_;  // TaskKey → TaskID
};
