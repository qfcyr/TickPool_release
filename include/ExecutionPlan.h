#pragma once
// ExecutionPlan.h —— 静态执行描述

#include <vector>
#include <cstdint>
#include <string>

// ========== Wave（从 TickPool.h 移入） ==========
struct Wave {
    uint32_t start;
    uint32_t count;
#ifndef NDEBUG
    std::string debugName;
#else
    const char* debugName = nullptr;
#endif
};

// ========== ExecutionPlan（纯拓扑编译产物） ==========
class ExecutionPlan {
public:
    using TaskID = uint32_t;
    static constexpr TaskID kInvalidTaskID = UINT32_MAX;

    // ---------- 编译器写入（GraphCompiler 使用） ----------
    void clearTopology() noexcept { taskOrder_.clear(); waves_.clear(); waveTasks_.clear(); }
    void clearWaves() noexcept { waves_.clear(); waveTasks_.clear(); }
    void addTaskOrder(TaskID id) { taskOrder_.push_back(id); }
    void addWave(const Wave& w) { waves_.push_back(w); }
    void addWaveTask(TaskID id) { waveTasks_.push_back(id); }

    // ---------- 运行期只读（TickRuntime / TickPool 使用） ----------
    const std::vector<TaskID>& taskOrder() const noexcept { return taskOrder_; }
    const std::vector<Wave>& waves() const noexcept { return waves_; }
    const std::vector<TaskID>& waveTasks() const noexcept { return waveTasks_; }
    size_t taskCount() const noexcept { return taskOrder_.size(); }

private:
    std::vector<TaskID> taskOrder_;
    std::vector<Wave> waves_;
    std::vector<TaskID> waveTasks_;
};
