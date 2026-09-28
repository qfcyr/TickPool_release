#pragma once
// PayloadCodec.h —— payload 序列化 codec

#include <vector>
#include <string>
#include <optional>
#include <utility>
#include <tuple>
#include <cstring>
#include <cstdint>
#include <type_traits>
#include <stdexcept>

using Bytes = std::vector<uint8_t>;

// ---------- 检测 ----------
template<typename T>
concept HasWriteRead = requires(const T& v, Bytes& b) {
    { v.Write() } -> std::convertible_to<Bytes>;
    const_cast<T&>(v).Read(b);   // Read 是非 const 成员
};

template<typename T>
struct is_std_vector : std::false_type {};
template<typename T, typename A>
struct is_std_vector<std::vector<T, A>> : std::true_type {};
template<typename T>
inline constexpr bool is_std_vector_v = is_std_vector<T>::value;

template<typename T>
struct is_std_optional : std::false_type {};
template<typename T>
struct is_std_optional<std::optional<T>> : std::true_type {};
template<typename T>
inline constexpr bool is_std_optional_v = is_std_optional<T>::value;

template<typename T>
struct is_std_pair : std::false_type {};
template<typename A, typename B>
struct is_std_pair<std::pair<A, B>> : std::true_type {};
template<typename T>
inline constexpr bool is_std_pair_v = is_std_pair<T>::value;

template<typename T>
struct is_std_tuple : std::false_type {};
template<typename... Ts>
struct is_std_tuple<std::tuple<Ts...>> : std::true_type {};
template<typename T>
inline constexpr bool is_std_tuple_v = is_std_tuple<T>::value;

// ---------- 写入（返回 Bytes） ----------
template<typename T> Bytes payloadWrite(const T& v);
template<typename T> void payloadReadInto(const Bytes& data, T& out);

// 追加一段字节
inline void appendBytes(Bytes& dst, Bytes&& src) {
    dst.insert(dst.end(), std::make_move_iterator(src.begin()), std::make_move_iterator(src.end()));
}
inline void appendBytes(Bytes& dst, const Bytes& src) {
    dst.insert(dst.end(), src.begin(), src.end());
}
inline void appendU64(Bytes& dst, uint64_t v) {
    dst.push_back(static_cast<uint8_t>(v & 0xFF));
    dst.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    dst.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    dst.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    dst.push_back(static_cast<uint8_t>((v >> 32) & 0xFF));
    dst.push_back(static_cast<uint8_t>((v >> 40) & 0xFF));
    dst.push_back(static_cast<uint8_t>((v >> 48) & 0xFF));
    dst.push_back(static_cast<uint8_t>((v >> 56) & 0xFF));
}

// 读取游标
struct PayloadReader {
    const Bytes& data;
    size_t pos = 0;
    Bytes take(size_t n) {
        if (pos + n > data.size()) throw std::runtime_error("PayloadCodec: truncated payload");
        Bytes out(data.begin() + static_cast<std::ptrdiff_t>(pos), data.begin() + static_cast<std::ptrdiff_t>(pos + n));
        pos += n;
        return out;
    }
    uint64_t takeU64() {
        if (pos + 8 > data.size()) throw std::runtime_error("PayloadCodec: truncated payload");
        uint64_t v = 0;
        for (int i = 7; i >= 0; --i) v = (v << 8) | data[pos + static_cast<size_t>(i)];
        pos += 8;
        return v;
    }
    // 游标式标量读取 —— 不构造临时 Bytes。
    uint64_t readLe(size_t n) {
        if (pos + n > data.size()) throw std::runtime_error("PayloadCodec: truncated payload");
        uint64_t acc = 0;
        for (size_t i = 0; i < n; ++i)
            acc |= static_cast<uint64_t>(data[pos + i]) << (8 * i);
        pos += n;
        return acc;
    }
    // 只返回指向源缓冲的指针（调用方需在推进游标后立即使用）
    const uint8_t* readPtr(size_t n) {
        if (pos + n > data.size()) throw std::runtime_error("PayloadCodec: truncated payload");
        const uint8_t* p = data.data() + pos;
        pos += n;
        return p;
    }
};

// tuple 递归写入
template<typename Tuple, size_t... I>
inline void writeTupleInto(Bytes& b, const Tuple& t, std::index_sequence<I...>) {
    (appendBytes(b, payloadWrite(std::get<I>(t))), ...);
}

template<typename T>
Bytes payloadWrite(const T& v) {
    if constexpr (std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T>) {
        // 1. byte-blit（主机字节序；仅限同机）
        Bytes b(sizeof(T));
        std::memcpy(b.data(), &v, sizeof(T));
        return b;
    }
    else if constexpr (std::same_as<std::decay_t<T>, std::string>) {
        Bytes b;
        appendU64(b, static_cast<uint64_t>(v.size()));
        b.insert(b.end(), reinterpret_cast<const uint8_t*>(v.data()),
            reinterpret_cast<const uint8_t*>(v.data()) + v.size());
        return b;
    }
    else if constexpr (is_std_vector_v<std::decay_t<T>>) {
        Bytes b;
        appendU64(b, static_cast<uint64_t>(v.size()));
        for (const auto& e : v) appendBytes(b, payloadWrite(e));
        return b;
    }
    else if constexpr (is_std_optional_v<std::decay_t<T>>) {
        Bytes b;
        appendU64(b, v.has_value() ? 1u : 0u);
        if (v.has_value()) appendBytes(b, payloadWrite(*v));
        return b;
    }
    else if constexpr (is_std_pair_v<std::decay_t<T>>) {
        Bytes b;
        appendBytes(b, payloadWrite(v.first));
        appendBytes(b, payloadWrite(v.second));
        return b;
    }
    else if constexpr (is_std_tuple_v<std::decay_t<T>>) {
        Bytes b;
        writeTupleInto(b, v, std::make_index_sequence<std::tuple_size_v<std::decay_t<T>>>{});
        return b;
    }
    else if constexpr (HasWriteRead<std::decay_t<T>>) {
        // 3. 用户 Write/Read：Write 输出经长度前缀包裹（支持嵌套容器）
        Bytes inner = const_cast<std::decay_t<T>&>(v).Write();
        Bytes b;
        appendU64(b, static_cast<uint64_t>(inner.size()));
        appendBytes(b, inner);
        return b;
    }
    else {
        static_assert(always_false_v<T>,
            "payload type not serializable: must be trivially copyable, or a built-in "
            "(string/vector/optional/pair/tuple), or provide Bytes Write() const and void Read(const Bytes&)");
    }
}

// tuple 递归读取（游标版本）
template<typename Tuple, size_t... I>
inline void readTupleFromImpl(PayloadReader& r, Tuple& t, std::index_sequence<I...>) {
    (payloadReadIntoImpl(r, std::get<I>(t)), ...);
}
template<typename T> void payloadReadIntoImpl(PayloadReader& r, T& out);

template<typename T>
void payloadReadIntoImpl(PayloadReader& r, T& out) {
    if constexpr (std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T>) {
        Bytes raw = r.take(sizeof(T));
        std::memcpy(&out, raw.data(), sizeof(T));
    }
    else if constexpr (std::same_as<std::decay_t<T>, std::string>) {
        uint64_t n = r.takeU64();
        Bytes raw = r.take(static_cast<size_t>(n));
        out.assign(reinterpret_cast<const char*>(raw.data()), raw.size());
    }
    else if constexpr (is_std_vector_v<std::decay_t<T>>) {
        uint64_t n = r.takeU64();
        out.clear();
        out.reserve(static_cast<size_t>(n));
        for (uint64_t i = 0; i < n; ++i) {
            typename std::decay_t<T>::value_type e;
            payloadReadIntoImpl(r, e);
            out.push_back(std::move(e));
        }
    }
    else if constexpr (is_std_optional_v<std::decay_t<T>>) {
        uint64_t has = r.takeU64();
        if (has) {
            typename std::decay_t<T>::value_type e;
            payloadReadIntoImpl(r, e);
            out = std::move(e);
        }
        else {
            out.reset();
        }
    }
    else if constexpr (is_std_pair_v<std::decay_t<T>>) {
        payloadReadIntoImpl(r, out.first);
        payloadReadIntoImpl(r, out.second);
    }
    else if constexpr (is_std_tuple_v<std::decay_t<T>>) {
        readTupleFromImpl(r, out, std::make_index_sequence<std::tuple_size_v<std::decay_t<T>>>{});
    }
    else if constexpr (HasWriteRead<std::decay_t<T>>) {
        uint64_t n = r.takeU64();
        Bytes raw = r.take(static_cast<size_t>(n));
        out.Read(raw);
    }
    else {
        static_assert(always_false_v<T>, "payload type not deserializable (see payloadWrite)");
    }
}

template<typename T>
void payloadReadInto(const Bytes& data, T& out) {
    PayloadReader r{ data, 0 };
    payloadReadIntoImpl(r, out);
}
