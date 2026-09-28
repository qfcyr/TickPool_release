#pragma once
// TickPayload.h —— 类型擦除的提交参数存储

// ops_，sizeof(Payload) 由 96B 降到 80B，且 **kSBOSize 保持 64 不变**（不改变任何 payload 的

// CommandEntry 由 128B 降到 112B；而 Task 每次提交要经 wheel → batch → tasksByOwner → 执行

#include <new>
#include <utility>
#include <type_traits>
#include <cstdint>

class Payload {
    static constexpr size_t kSBOSize = 64;

    // 类型擦除操作表：每个 T 一张静态表（函数局部 static，线程安全初始化）。
    struct Ops {
        void (*destroy)(void*) noexcept;
        void (*move)(void* dst, void* src) noexcept;
        void* (*cloneInto)(void* dstStorage, const void* src);
    };

    template<typename T>
    static constexpr bool kSboEligible =
        sizeof(T) <= kSBOSize && alignof(T) <= alignof(std::max_align_t);

    alignas(16) char storage_[kSBOSize];
    void* ptr_ = storage_;        // 指向有效对象（SBO 或堆）
    const Ops* ops_ = nullptr;

    template<typename T>
    static const Ops* opsFor() noexcept {
        static const Ops kOps{
            // destroy
            [](void* p) noexcept { static_cast<T*>(p)->~T(); },
            // move：SBO 就地移动构造并析构源；堆则搬指针
            [](void* dst, void* src) noexcept {
                if constexpr (kSboEligible<T>) {
                    ::new (dst) T(std::move(*static_cast<T*>(src)));
                    static_cast<T*>(src)->~T();
                }
                else {
                    *reinterpret_cast<void**>(dst) = *reinterpret_cast<void**>(src);
                }
            },
            // cloneInto：显式深拷贝（命令日志在提交点记录参数用）
            [](void* dstStorage, const void* src) -> void* {
                if constexpr (kSboEligible<T>) {
                    ::new (dstStorage) T(*static_cast<const T*>(src));
                    return dstStorage;
                }
                else {
                    return new T(*static_cast<const T*>(src));
                }
            }
        };
        return &kOps;
    }

public:
    Payload() noexcept = default;

    template<typename T, typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, Payload>>>
    explicit Payload(T&& v) { emplace<std::decay_t<T>>(std::forward<T>(v)); }

    Payload(const Payload&) = delete;
    Payload& operator=(const Payload&) = delete;

    Payload(Payload&& o) noexcept { moveFrom(o); }
    Payload& operator=(Payload&& o) noexcept {
        if (this != &o) { clear(); moveFrom(o); }
        return *this;
    }

    ~Payload() { clear(); }

    // 显式深拷贝（保持 move-only 语义；命令日志在提交点复制参数用）
    Payload clone() const {
        Payload out;
        if (!ops_) return out;
        out.ptr_ = ops_->cloneInto(out.storage_, ptr_);
        out.ops_ = ops_;
        return out;
    }

    void* get() noexcept { return ptr_; }
    const void* get() const noexcept { return ptr_; }
    explicit operator bool() const noexcept { return ops_ != nullptr; }

private:
    template<typename T, typename... Args>
    void emplace(Args&&... args) {
        static_assert(!std::is_reference_v<T>, "Payload must hold a value type");
        if constexpr (kSboEligible<T>) {
            ptr_ = storage_;
            new (storage_) T(std::forward<Args>(args)...);
        }
        else {
            ptr_ = new T(std::forward<Args>(args)...);
        }
        ops_ = opsFor<T>();
    }

    void moveFrom(Payload& o) noexcept {
        if (!o.ops_) return;                   // 空 payload
        if (o.ptr_ == o.storage_) {
            ptr_ = storage_;                   // SBO：move 构造（会析构源对象）
            o.ops_->move(ptr_, o.ptr_);
        }
        else {
            ptr_ = o.ptr_;                     // 堆：搬指针
        }
        ops_ = o.ops_;
        o.ops_ = nullptr;
        o.ptr_ = o.storage_;
    }

    void clear() noexcept {
        if (ops_) {
            ops_->destroy(ptr_);
            ops_ = nullptr;
            ptr_ = storage_;
        }
    }
};

// 不变量：三个函数指针折叠成一个表指针后，Payload 必须是 80B。
static_assert(sizeof(Payload) <= 80,
    "Payload metadata regression: destroy/move/cloneInto must stay folded into one ops-table pointer (M12)");
