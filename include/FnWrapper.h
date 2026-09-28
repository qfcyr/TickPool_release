#pragma once
#include <new>
#include <utility>
#include <type_traits>
#include <cstring>

// ========== ：Fn 堆分配计数（分段计时 Profile 的一部分） ==========
#if !defined(TICKPOOL_ENABLE_PROFILE)
#  ifdef NDEBUG
#    define TICKPOOL_ENABLE_PROFILE 0
#  else
#    define TICKPOOL_ENABLE_PROFILE 1
#  endif
#endif
#if TICKPOOL_ENABLE_PROFILE
#  include <atomic>
#  include <cstdint>
namespace tickpool_detail { inline std::atomic<uint64_t> g_fnHeapAllocs{ 0 }; }
#endif

// ========== 轻量级可调用对象 Fn ==========

// 可调用对象（捕获 std::string/Payload 的 lambda 等）一律 new/delete 堆分配；
struct Fn {
    static constexpr size_t kBufferSize = 64;

    void (*execute)(void* storage) = nullptr;   // 非 noexcept —— 用户 construct/destruct 异常须可传播给包装层策略
    void (*destroy)(void* storage) noexcept = nullptr;      // SBO：就地析构 storage 处对象；heap：从 storage 读指针后 delete
    void (*moveInto)(void* dstStorage, void* srcStorage) noexcept = nullptr;  // SBO：dst 上 move 构造 + src 就地析构
    // Release 不再默认清零 storage。原 `= {}` 让每次默认构造 Fn 都 memset 64 字节，
    // 而线程池轮询/窃取路径会频繁默认构造该对象、且在 dequeue 失败时整块丢弃。
#ifndef NDEBUG
    alignas(16) char storage[kBufferSize] = {};   // Debug：保守清零，便于排查
#else
    alignas(16) char storage[kBufferSize];        // Release：不清零
#endif
    bool heap = false;

    Fn() noexcept = default;

    // 允许从任意可调用对象隐式构造
    template<typename F>
        requires (!std::is_same_v<std::remove_cvref_t<F>, Fn>)
    Fn(F&& f) : execute(nullptr), destroy(nullptr), moveInto(nullptr), heap(false) {
        using CallableType = std::remove_cvref_t<F>;
        if constexpr (sizeof(CallableType) <= kBufferSize &&
            alignof(CallableType) <= alignof(std::max_align_t))
        {
            // SBO：就地构造；非平凡析构类型同样支持（destroy 就地析构，moveInto 就地移动）
            static_assert(std::is_move_constructible_v<CallableType>,
                "Fn SBO requires a move-constructible callable");
            execute = [](void* self) {
                (*static_cast<CallableType*>(self))();
            };
            if constexpr (std::is_trivially_destructible_v<CallableType>) {
                destroy = nullptr;
            }
            else {
                destroy = [](void* self) noexcept {
                    static_cast<CallableType*>(self)->~CallableType();
                };
            }
            moveInto = [](void* dst, void* src) noexcept {
                ::new (dst) CallableType(std::move(*static_cast<CallableType*>(src)));
                static_cast<CallableType*>(src)->~CallableType();
            };
            new (storage) CallableType(std::forward<F>(f));
        }
        else {
            // 堆后备（对象过大或对齐过高）：storage 仅存指针，移动走指针搬移
            heap = true;
            auto* heapPtr = new CallableType(std::forward<F>(f));
#if TICKPOOL_ENABLE_PROFILE
            tickpool_detail::g_fnHeapAllocs.fetch_add(1, std::memory_order_relaxed);
#endif
            execute = [](void* self) {
                auto& callable = *static_cast<CallableType*>(
                    reinterpret_cast<void**>(self)[0]);
                callable();
            };
            destroy = [](void* self) noexcept {
                auto* callablePtr = static_cast<CallableType*>(
                    reinterpret_cast<void**>(self)[0]);
                delete callablePtr;
            };
            *reinterpret_cast<void**>(storage) = heapPtr;
        }
    }

    Fn(const Fn&) = delete;
    Fn& operator=(const Fn&) = delete;

    Fn(Fn&& other) noexcept
        : execute(other.execute), destroy(other.destroy), moveInto(other.moveInto), heap(other.heap)
    {
        if (heap) {
            *reinterpret_cast<void**>(storage) = *reinterpret_cast<void**>(other.storage);
        }
        else if (moveInto) {
            moveInto(storage, other.storage);   // SBO：就地移动（含析构源对象）
        }
        other.execute = nullptr;
        other.destroy = nullptr;
        other.moveInto = nullptr;
        other.heap = false;
    }

    Fn& operator=(Fn&& other) noexcept {
        if (this != &other) {
            clear();
            execute = other.execute;
            destroy = other.destroy;
            moveInto = other.moveInto;
            heap = other.heap;
            if (heap) {
                *reinterpret_cast<void**>(storage) = *reinterpret_cast<void**>(other.storage);
            }
            else if (moveInto) {
                moveInto(storage, other.storage);
            }
            other.execute = nullptr;
            other.destroy = nullptr;
            other.moveInto = nullptr;
            other.heap = false;
        }
        return *this;
    }

    ~Fn() noexcept { clear(); }

    void operator()() const {   // 非 noexcept（异常交由调用方策略处理）
        if (execute) {
            void* self = const_cast<void*>(static_cast<const void*>(storage));
            execute(self);
        }
    }

    explicit operator bool() const noexcept { return execute != nullptr; }

private:
    void clear() noexcept {
        if (destroy) {
            // SBO：就地析构 storage 处对象；heap：destroy 从 storage 读指针并 delete
            destroy(storage);
        }
        execute = nullptr;
        destroy = nullptr;
        moveInto = nullptr;
        heap = false;
    }
};
