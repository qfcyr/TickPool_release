#pragma once
// ActionRegistry.h —— 独立行为注册表

#include <string>
#include <vector>
#include <utility>
#include <stdexcept>
#include <cstdint>
#include <ankerl/unordered_dense.h>

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
class ActionRegistry {
public:
    using Context = TaskContext<Duration, OptionsT, ThreadPoolType>;
    using ActionDeclT = ActionDecl<Duration, OptionsT, ThreadPoolType>;
    using ActionID = uint32_t;

    // 编译期配置
    static constexpr OptionsT options{};

    // 确定性模式下的结果队列条目：{seq, result}；关闭时直接存 result
    template<typename T>
    using SeqItem = std::conditional_t<options.enableDeterministicOrdering, std::pair<uint64_t, T>, T>;

    ActionID registerAction(const std::string& fullName, ActionDeclT&& decl) {
        if (byName_.find(fullName) != byName_.end())
            throw std::invalid_argument("action already registered: " + fullName);
        byName_[fullName] = static_cast<ActionID>(actions_.size());
        actions_.push_back(std::move(decl));
        return static_cast<ActionID>(actions_.size() - 1);
    }

    bool hasAction(const std::string& fullName) const noexcept {
        return byName_.find(fullName) != byName_.end();
    }

    ActionID actionId(const std::string& fullName) const {
        auto it = byName_.find(fullName);
        if (it == byName_.end()) throw std::invalid_argument("action not registered: " + fullName);
        return it->second;
    }

    bool isAsync(ActionID id) const noexcept { return actions_[id].isAsync; }
    std::type_index payloadType(ActionID id) const noexcept { return actions_[id].payloadType; }
    const std::string& actionName(ActionID id) const noexcept { return actions_[id].name; }

    // payload 序列化/反序列化（快照用）
    Bytes writePayload(ActionID id, const void* payload) const {
        return actions_[id].writePayload(payload);
    }
    ::Payload makePayloadFrom(ActionID id, const Bytes& data) const {
        if (!actions_[id].makePayloadFromBytes)
            throw std::invalid_argument("action payload not restorable (no codec): " + actions_[id].name);
        return actions_[id].makePayloadFromBytes(data);
    }

    // 网络协议 payload 序列化/反序列化（可移植：内置/标量/Write-Read，禁止 byte-blit）
    Bytes writePayloadNet(ActionID id, const void* payload) const {
        return actions_[id].writePayloadNet(payload);
    }
    ::Payload makePayloadFromNet(ActionID id, const Bytes& data) const {
        if (!actions_[id].makePayloadFromNetBytes)
            throw std::invalid_argument("action payload not network-restorable (no net codec): " + actions_[id].name);
        return actions_[id].makePayloadFromNetBytes(data);
    }

    // 并行 action 执行（延迟；结果入 ctx.parallelResultQueue）
    void execute(ActionID id, Context& ctx, const void* payload) const {
        const ActionDeclT& e = actions_[id];
        if (e.execute) e.execute(e.box, ctx, payload);
    }

    // 异步 action 执行（立即；结果入 asyncQueue）
    void executeAsync(ActionID id, Context& ctx, const void* payload, void* asyncQueue) const {
        const ActionDeclT& e = actions_[id];
        if (e.executeAsync) e.executeAsync(e.box, ctx, payload, asyncQueue);
    }

    // ---------- 工厂（注册时实例化；ResultType = 任务对应结果类型） ----------
    template<typename Callable, typename Params, typename ResultType>
    ActionDeclT makeParallel(std::string name, Callable&& c) const {
        using PayloadT = std::conditional_t<std::is_void_v<Params>, std::monostate, Params>;
        using Box = ActionWorkBox<std::decay_t<Callable>>;
        ActionDeclT d;
        d.name = std::move(name);
        d.isAsync = false;
        d.payloadType = typeid(PayloadT);
        d.writePayload = [](const void* p) -> Bytes { return payloadWrite(*static_cast<const PayloadT*>(p)); };
        d.readPayload = [](const Bytes& b, void* out) { payloadReadInto(b, *static_cast<PayloadT*>(out)); };
        d.makePayloadFromBytes = [](const Bytes& b) -> ::Payload {
            PayloadT v;
            payloadReadInto(b, v);
            return ::Payload(std::move(v));
        };
        // 网络协议 codec（可移植；byte-blit 类型编译失败 → 提示补 Write/Read）
        // 本组属于**网络子系统**（NetworkCodec.h），随 TICKPOOL_ENABLE_NETWORK 一起编译掉；
#if TICKPOOL_ENABLE_NETWORK
        d.writePayloadNet = [](const void* p) -> Bytes { return payloadWriteNet(*static_cast<const PayloadT*>(p)); };
        d.readPayloadNet = [](const Bytes& b, void* out) { payloadReadNetInto(b, *static_cast<PayloadT*>(out)); };
        d.makePayloadFromNetBytes = [](const Bytes& b) -> ::Payload {
            PayloadT v;
            payloadReadNetInto(b, v);
            return ::Payload(std::move(v));
        };
#endif
        auto* box = new Box(std::forward<Callable>(c));
        d.box = box;
        d.destroyBox = [](void* b) noexcept { delete static_cast<Box*>(b); };
        // 非 noexcept —— 让用户 action 的异常能传播到接住点（并行：runSubTaskBody；异步：提交分支），
        d.execute = [](void* b, Context& ctx, const void* payload) {
            auto& fn = static_cast<Box*>(b)->fn;
            const auto& p = *static_cast<const PayloadT*>(payload);
            using Result = ResultType;
            if constexpr (std::is_void_v<Result>) {
                actionInvoke(fn, ctx, p);
            }
            else {
                Result r = actionInvoke(fn, ctx, p);
                auto* q = static_cast<moodycamel::ConcurrentQueue<SeqItem<Result>>*>(ctx.parallelResultQueue);
                if constexpr (options.enableDeterministicOrdering) q->enqueue({ ctx.seq, std::move(r) });
                else q->enqueue(std::move(r));
            }
        };
        d.executeAsync = nullptr;
        return d;
    }

    template<typename Callable, typename Params, typename ResultType>
    ActionDeclT makeAsync(std::string name, Callable&& c) const {
        using PayloadT = std::conditional_t<std::is_void_v<Params>, std::monostate, Params>;
        using Box = ActionWorkBox<std::decay_t<Callable>>;
        ActionDeclT d;
        d.name = std::move(name);
        d.isAsync = true;
        d.payloadType = typeid(PayloadT);
        d.writePayload = [](const void* p) -> Bytes { return payloadWrite(*static_cast<const PayloadT*>(p)); };
        d.readPayload = [](const Bytes& b, void* out) { payloadReadInto(b, *static_cast<PayloadT*>(out)); };
        d.makePayloadFromBytes = [](const Bytes& b) -> ::Payload {
            PayloadT v;
            payloadReadInto(b, v);
            return ::Payload(std::move(v));
        };
        // 网络协议 codec（可移植；byte-blit 类型编译失败 → 提示补 Write/Read）
        // 本组属于**网络子系统**（NetworkCodec.h），随 TICKPOOL_ENABLE_NETWORK 一起编译掉；
#if TICKPOOL_ENABLE_NETWORK
        d.writePayloadNet = [](const void* p) -> Bytes { return payloadWriteNet(*static_cast<const PayloadT*>(p)); };
        d.readPayloadNet = [](const Bytes& b, void* out) { payloadReadNetInto(b, *static_cast<PayloadT*>(out)); };
        d.makePayloadFromNetBytes = [](const Bytes& b) -> ::Payload {
            PayloadT v;
            payloadReadNetInto(b, v);
            return ::Payload(std::move(v));
        };
#endif
        auto* box = new Box(std::forward<Callable>(c));
        d.box = box;
        d.destroyBox = [](void* b) noexcept { delete static_cast<Box*>(b); };
        d.execute = nullptr;
        // 同上，非 noexcept
        d.executeAsync = [](void* b, Context& ctx, const void* payload, void* asyncQueue) {
            auto& fn = static_cast<Box*>(b)->fn;
            const auto& p = *static_cast<const PayloadT*>(payload);
            using Result = ResultType;
            if constexpr (std::is_void_v<Result>) {
                actionInvoke(fn, ctx, p);
                auto* q = static_cast<TypedAsyncQueue<SeqItem<std::monostate>, options.asyncQueueCapacity>*>(asyncQueue);
                if constexpr (options.enableDeterministicOrdering) q->enqueue({ ctx.seq, std::monostate{} });
                else q->enqueue(std::monostate{});
            }
            else {
                Result r = actionInvoke(fn, ctx, p);
                auto* q = static_cast<TypedAsyncQueue<SeqItem<Result>, options.asyncQueueCapacity>*>(asyncQueue);
                if constexpr (options.enableDeterministicOrdering) q->enqueue({ ctx.seq, std::move(r) });
                else q->enqueue(std::move(r));
            }
        };
        return d;
    }

private:
    std::vector<ActionDeclT> actions_;
    ankerl::unordered_dense::map<std::string, ActionID> byName_;
};
