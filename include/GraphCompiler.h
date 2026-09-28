#pragma once
// GraphCompiler.h —— 编译期

#include <queue>
#include <algorithm>
#include <stdexcept>

template<DefaultTickPoolOptions /*options*/>
class GraphCompiler {
public:
    // 输入：TaskRegistry（定义身份：id→key，含哨兵任务）+ 冷数据表（含 deps 配置）；
    void compile(
        const TaskRegistry& registry,
        const std::vector<TaskColdData>& coldTable,
        ExecutionPlan& plan) const
    {
        using TaskID = ExecutionPlan::TaskID;
        const std::vector<TaskKey>& idToKey = registry.idToKey();
        const TaskKey startKey = "TickConstructionTask";
        const TaskKey endKey = "TickDestructionTask";

        // 本地 TaskKey → TaskID（id 即定义顺序索引）
        ankerl::unordered_dense::map<TaskKey, TaskID, std::hash<std::string>> idOf;
        idOf.reserve(idToKey.size());
        for (TaskID id = 0; id < static_cast<TaskID>(idToKey.size()); ++id)
            idOf.emplace(idToKey[id], id);

        // ---------- 依赖边的**唯一**枚举点 ----------
        auto forEachDependencyEdge = [&]<typename F>(F&& fn) {
            for (TaskID id = 0; id < static_cast<TaskID>(idToKey.size()); ++id) {
                const TaskKey& key = idToKey[id];
                const TaskColdData& cold = coldTable[id];
                for (const auto& before : cold.desc.deps.before) fn(before, key);
                for (const auto& after : cold.desc.deps.after) fn(key, after);
            }
        };

        // ---------- DAG 构建与拓扑排序 ----------
        ankerl::unordered_dense::map<TaskKey, std::vector<TaskKey>, std::hash<std::string>> dependents;
        ankerl::unordered_dense::map<TaskKey, int, std::hash<std::string>> inDegree;
        for (TaskID id = 0; id < static_cast<TaskID>(idToKey.size()); ++id)
            inDegree[idToKey[id]] = 0;

        forEachDependencyEdge([&](const TaskKey& from, const TaskKey& to) {
            dependents[from].push_back(to);
            inDegree[to]++;
        });
        // 哨兵边是引擎内部的合成导线（start → 全部零入度任务；无后继者 → end），只注入拓扑图。
        for (TaskID id = 0; id < static_cast<TaskID>(idToKey.size()); ++id) {
            const TaskKey& key = idToKey[id];
            if (key == startKey || key == endKey) continue;
            const TaskColdData& cold = coldTable[id];
            if (inDegree[key] == 0 && cold.desc.deps.before.empty()) {
                dependents[startKey].push_back(key);
                inDegree[key]++;
            }
            if (cold.desc.deps.after.empty()) {
                dependents[key].push_back(endKey);
                inDegree[endKey]++;
            }
        }

        // 零入度任务按定义顺序（TaskID）排序入队，消除 hash map 迭代顺序对
        // taskOrder / waves 的影响，保证同一输入两次编译产出同一 Plan（可复现性）。
        std::vector<TaskKey> zeroDegree;
        for (auto& [k, deg] : inDegree) if (deg == 0) zeroDegree.push_back(k);
        std::sort(zeroDegree.begin(), zeroDegree.end(),
            [&](const TaskKey& a, const TaskKey& b) { return idOf.at(a) < idOf.at(b); });
        std::queue<TaskKey> q;
        for (auto& k : zeroDegree) q.push(k);

        plan.clearTopology();
        while (!q.empty()) {
            auto cur = q.front(); q.pop();
            TaskID id = idOf.at(cur);
            plan.addTaskOrder(id);
            for (auto& dep : dependents[cur])
                if (--inDegree[dep] == 0) q.push(dep);
        }
        if (plan.taskOrder().size() != idToKey.size())
            throw std::runtime_error("Cyclic dependency detected");

        // ---------- 波次推导 ----------
        plan.clearWaves();
        ankerl::unordered_dense::map<TaskKey, int, std::hash<std::string>> waveInDegree;
        ankerl::unordered_dense::map<TaskKey, std::vector<TaskKey>, std::hash<std::string>> waveDependents;
        for (TaskID id = 0; id < static_cast<TaskID>(idToKey.size()); ++id)
            waveInDegree[idToKey[id]] = 0;
        // 与拓扑**同源**：before/after 在此同等生效（两者都进同一边集）。
        forEachDependencyEdge([&](const TaskKey& from, const TaskKey& to) {
            waveDependents[from].push_back(to);
            waveInDegree[to]++;
        });
        // 同上，波次推导的零入度队列也按定义顺序入队
        std::vector<TaskKey> waveZero;
        for (auto& [k, deg] : waveInDegree) if (deg == 0) waveZero.push_back(k);
        std::sort(waveZero.begin(), waveZero.end(),
            [&](const TaskKey& a, const TaskKey& b) { return idOf.at(a) < idOf.at(b); });
        std::queue<TaskKey> wq;
        for (auto& k : waveZero) wq.push(k);
        while (!wq.empty()) {
            size_t waveSize = wq.size();
            Wave w;
            w.start = static_cast<uint32_t>(plan.waveTasks().size());
            w.count = static_cast<uint32_t>(waveSize);
#ifndef NDEBUG
            TaskKey first = wq.front();
            w.debugName = first;
#endif
            for (size_t i = 0; i < waveSize; ++i) {
                TaskKey cur = wq.front(); wq.pop();
                TaskID id = idOf.at(cur);
                plan.addWaveTask(id);
                for (const auto& dep : waveDependents[cur])
                    if (--waveInDegree[dep] == 0) wq.push(dep);
            }
            plan.addWave(w);
        }
    }
};
