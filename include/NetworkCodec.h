#pragma once
// NetworkCodec.h —— 网络协议字节编解码（命令日志 + 世界传输）

#include "PayloadCodec.h"     // Bytes / appendU64 / PayloadReader / 内置类型检测
#include "SnapshotCodec.h"    // snap_detail（putU32/putU64/putStr/putBytes/getStr/getBytes）+ crc32
#include <variant>            // std::monostate（void payload）

inline constexpr uint32_t TICKPOOL_NET_PROTOCOL_VERSION = 1;

inline const char* kNetMagicCommands = "TICKPOOL_CMD_V1";    // 15 字符（写入时零填充到 16）
inline const char* kNetMagicWorld    = "TICKPOOL_WORLDV1";   // 16 字符

// ======================================================================
template<typename T> Bytes payloadWriteNet(const T& v);
template<typename T> void payloadReadNetInto(const Bytes& data, T& out);

// ---------- 目标缓冲优先写入 ----------

// 「每元素 2 次分配 + 2 次释放」：vector<uint32_t>(1000000) ≈ 200 万次分配。

inline void appendNetRawLe(Bytes& dst, uint64_t raw, size_t n) {
    const size_t old = dst.size();
    dst.resize(old + n);
    for (size_t i = 0; i < n; ++i) dst[old + i] = static_cast<uint8_t>((raw >> (8 * i)) & 0xFF);
}

template<typename T>
inline void appendNet(Bytes& dst, const T& v) {
    using U = std::decay_t<T>;
    if constexpr (std::same_as<U, std::monostate>) {
        // void payload：空类型，零字节编码
    }
    else if constexpr (std::is_floating_point_v<U>) {
        // IEEE754 位模式 + 固定宽度 little-endian（可移植）
        if constexpr (sizeof(U) == 4) { uint32_t bits; std::memcpy(&bits, &v, 4); appendNetRawLe(dst, bits, 4); }
        else { uint64_t bits; std::memcpy(&bits, &v, 8); appendNetRawLe(dst, bits, 8); }
    }
    else if constexpr (std::is_enum_v<U>) {
        using UT = std::underlying_type_t<U>;
        appendNetRawLe(dst, static_cast<uint64_t>(static_cast<UT>(v)), sizeof(UT));
    }
    else if constexpr (std::is_arithmetic_v<U>) {
        // 整数/布尔/字符：固定宽度 little-endian（负值按补码低 sizeof(U) 字节）
        appendNetRawLe(dst, static_cast<uint64_t>(v), sizeof(U));
    }
    else if constexpr (std::same_as<U, std::string>) {
        appendU64(dst, static_cast<uint64_t>(v.size()));
        dst.insert(dst.end(), reinterpret_cast<const uint8_t*>(v.data()),
            reinterpret_cast<const uint8_t*>(v.data()) + v.size());
    }
    else {
        appendBytes(dst, payloadWriteNet(v));   // 容器 / optional / pair / tuple / 用户 Write-Read
    }
}

template<typename Tuple, size_t... I>
inline void writeTupleNet(Bytes& b, const Tuple& t, std::index_sequence<I...>) {
    (appendNet(b, std::get<I>(t)), ...);
}

template<typename T>
Bytes payloadWriteNet(const T& v) {
    using U = std::decay_t<T>;
    if constexpr (std::same_as<U, std::string>) {
        Bytes b;
        appendU64(b, static_cast<uint64_t>(v.size()));
        b.insert(b.end(), reinterpret_cast<const uint8_t*>(v.data()),
            reinterpret_cast<const uint8_t*>(v.data()) + v.size());
        return b;
    }
    else if constexpr (is_std_vector_v<U>) {
        Bytes b;
        appendU64(b, static_cast<uint64_t>(v.size()));
        for (const auto& e : v) appendNet(b, e);
        return b;
    }
    else if constexpr (is_std_optional_v<U>) {
        Bytes b;
        appendU64(b, v.has_value() ? 1u : 0u);
        if (v.has_value()) appendNet(b, *v);   // 直写
        return b;
    }
    else if constexpr (is_std_pair_v<U>) {
        Bytes b;
        appendNet(b, v.first);    // 直写
        appendNet(b, v.second);
        return b;
    }
    else if constexpr (is_std_tuple_v<U>) {
        Bytes b;
        writeTupleNet(b, v, std::make_index_sequence<std::tuple_size_v<U>>{});
        return b;
    }
    else if constexpr (std::same_as<U, std::monostate>) {
        // void payload：空类型，零字节编码（读取侧同样不消费字节）
        return Bytes{};
    }
    else if constexpr (HasWriteRead<U>) {
        // 用户 Write()/Read() 显式契约（优先于平凡复制 —— 网络传输必须显式）
        Bytes inner = const_cast<U&>(v).Write();
        Bytes b;
        appendU64(b, static_cast<uint64_t>(inner.size()));
        appendBytes(b, inner);
        return b;
    }
    else if constexpr (std::is_floating_point_v<U>) {
        // IEEE754 位模式 + 固定宽度 little-endian（可移植）
        Bytes b(sizeof(U));
        if constexpr (sizeof(U) == 4) {
            uint32_t bits; std::memcpy(&bits, &v, 4);
            for (size_t i = 0; i < 4; ++i) b[i] = static_cast<uint8_t>((bits >> (8 * i)) & 0xFF);
        }
        else {
            uint64_t bits; std::memcpy(&bits, &v, 8);
            for (size_t i = 0; i < 8; ++i) b[i] = static_cast<uint8_t>((bits >> (8 * i)) & 0xFF);
        }
        return b;
    }
    else if constexpr (std::is_enum_v<U>) {
        using UT = std::underlying_type_t<U>;
        Bytes b(sizeof(UT));
        uint64_t raw = static_cast<uint64_t>(static_cast<UT>(v));
        for (size_t i = 0; i < sizeof(UT); ++i)
            b[i] = static_cast<uint8_t>((raw >> (8 * i)) & 0xFF);
        return b;
    }
    else if constexpr (std::is_arithmetic_v<U>) {
        // 整数/布尔/字符：固定宽度 little-endian（负值按补码低 sizeof(U) 字节）
        Bytes b(sizeof(U));
        uint64_t raw = static_cast<uint64_t>(v);
        for (size_t i = 0; i < sizeof(U); ++i)
            b[i] = static_cast<uint8_t>((raw >> (8 * i)) & 0xFF);
        return b;
    }
    else {
        static_assert(always_false_v<T>,
            "payload type not network-portable: provide Bytes Write() const and void Read(const Bytes&), "
            "or use built-in (string/vector/optional/pair/tuple) / scalar types. "
            "Raw byte-blit (trivially copyable structs) is same-machine only (snapshot/rollback).");
    }
}

// tuple 递归读取
template<typename Tuple, size_t... I>
inline void readTupleNetImpl(PayloadReader& r, Tuple& t, std::index_sequence<I...>) {
    (payloadReadNetIntoImpl(r, std::get<I>(t)), ...);
}
template<typename T> void payloadReadNetIntoImpl(PayloadReader& r, T& out);

template<typename T>
void payloadReadNetIntoImpl(PayloadReader& r, T& out) {
    using U = std::decay_t<T>;
    if constexpr (std::same_as<U, std::string>) {
        const uint64_t n = r.takeU64();
        const uint8_t* p = r.readPtr(static_cast<size_t>(n));   // 不再复制一份 Bytes
        out.assign(reinterpret_cast<const char*>(p), static_cast<size_t>(n));
    }
    else if constexpr (is_std_vector_v<U>) {
        uint64_t n = r.takeU64();
        out.clear();
        out.reserve(static_cast<size_t>(n));
        for (uint64_t i = 0; i < n; ++i) {
            typename U::value_type e;
            payloadReadNetIntoImpl(r, e);
            out.push_back(std::move(e));
        }
    }
    else if constexpr (is_std_optional_v<U>) {
        uint64_t has = r.takeU64();
        if (has) {
            typename U::value_type e;
            payloadReadNetIntoImpl(r, e);
            out = std::move(e);
        }
        else out.reset();
    }
    else if constexpr (is_std_pair_v<U>) {
        payloadReadNetIntoImpl(r, out.first);
        payloadReadNetIntoImpl(r, out.second);
    }
    else if constexpr (is_std_tuple_v<U>) {
        readTupleNetImpl(r, out, std::make_index_sequence<std::tuple_size_v<U>>{});
    }
    else if constexpr (std::same_as<U, std::monostate>) {
        // void payload：零字节，无操作
    }
    else if constexpr (HasWriteRead<U>) {
        uint64_t n = r.takeU64();
        Bytes raw = r.take(static_cast<size_t>(n));
        out.Read(raw);
    }
    else if constexpr (std::is_floating_point_v<U>) {
        const uint64_t acc = r.readLe(sizeof(U));   // 游标式，无临时 Bytes
        if constexpr (sizeof(U) == 4) { uint32_t bits = static_cast<uint32_t>(acc); std::memcpy(&out, &bits, 4); }
        else { uint64_t bits = acc; std::memcpy(&out, &bits, 8); }
    }
    else if constexpr (std::is_enum_v<U>) {
        using UT = std::underlying_type_t<U>;
        out = static_cast<U>(static_cast<UT>(r.readLe(sizeof(UT))));
    }
    else if constexpr (std::is_arithmetic_v<U>) {
        out = static_cast<U>(r.readLe(sizeof(U)));
    }
    else {
        static_assert(always_false_v<T>, "payload type not network-deserializable (see payloadWriteNet)");
    }
}

template<typename T>
void payloadReadNetInto(const Bytes& data, T& out) {
    PayloadReader r{ data, 0 };
    payloadReadNetIntoImpl(r, out);
}

// ======================================================================
struct NetCommand {
    uint64_t submitTick = 0;
    uint64_t executeTick = 0;
    std::string task;
    std::string action;
    Bytes payload;
};

struct NetUserSection {
    uint8_t scenario = 0;
    Bytes data;
};

struct NetWorldData {
    uint64_t schemaVersion = 0;
    uint64_t tickCount = 0;
    std::vector<NetCommand> submissions;
    std::vector<NetUserSection> userSections;
};

// ---------- 编码辅助 ----------
namespace net_detail {

    // 写入 16 字节 magic（字符串零填充；字符串长度必须 ≤ 16）
    inline void putMagic(Bytes& b, const char* m) {
        for (int i = 0; i < 16; ++i)
            b.push_back(m[i] ? static_cast<uint8_t>(m[i]) : 0);
    }
    inline std::string readMagic(const Bytes& b, size_t& pos) {
        if (pos + 16 > b.size()) throw std::runtime_error("Net: truncated (magic)");
        std::string m(b.begin() + static_cast<std::ptrdiff_t>(pos),
            b.begin() + static_cast<std::ptrdiff_t>(pos + 16));
        pos += 16;
        return m;
    }
    inline std::string expectedMagic(const char* m) {
        std::string s(m);
        s.resize(16, '\0');
        return s;
    }
    inline void putVersion(Bytes& b) { snap_detail::putU32(b, TICKPOOL_NET_PROTOCOL_VERSION); }
    inline uint32_t getVersion(const Bytes& b, size_t& pos) {
        uint32_t v = snap_detail::getU32(b, pos);
        if (v != TICKPOOL_NET_PROTOCOL_VERSION)
            throw std::runtime_error("Net: protocol version mismatch (got " + std::to_string(v) +
                ", expected " + std::to_string(TICKPOOL_NET_PROTOCOL_VERSION) + ")");
        return v;
    }
    inline void putCommand(Bytes& b, const NetCommand& c) {
        snap_detail::putU64(b, c.submitTick);
        snap_detail::putU64(b, c.executeTick);
        snap_detail::putStr(b, c.task);
        snap_detail::putStr(b, c.action);
        snap_detail::putBytes(b, c.payload);
    }
    inline NetCommand getCommand(const Bytes& b, size_t& pos) {
        NetCommand c;
        c.submitTick = snap_detail::getU64(b, pos);
        c.executeTick = snap_detail::getU64(b, pos);
        c.task = snap_detail::getStr(b, pos);
        c.action = snap_detail::getStr(b, pos);
        c.payload = snap_detail::getBytes(b, pos);
        return c;
    }
    inline void putSubmission(Bytes& b, const NetCommand& c) {
        snap_detail::putStr(b, c.task);
        snap_detail::putStr(b, c.action);
        snap_detail::putU64(b, c.executeTick);
        snap_detail::putU64(b, c.submitTick);
        snap_detail::putBytes(b, c.payload);
    }
    inline NetCommand getSubmission(const Bytes& b, size_t& pos) {
        NetCommand c;
        c.task = snap_detail::getStr(b, pos);
        c.action = snap_detail::getStr(b, pos);
        c.executeTick = snap_detail::getU64(b, pos);
        c.submitTick = snap_detail::getU64(b, pos);
        c.payload = snap_detail::getBytes(b, pos);
        return c;
    }

} // namespace net_detail

// ======================================================================
inline Bytes encodeCommands(const std::vector<NetCommand>& cmds) {
    Bytes b;
    net_detail::putMagic(b, kNetMagicCommands);
    net_detail::putVersion(b);
    snap_detail::putU64(b, cmds.size());
    for (const auto& c : cmds) net_detail::putCommand(b, c);
    uint32_t crc = crc32(b.data(), b.size());
    snap_detail::putU32(b, crc);
    return b;
}

inline std::vector<NetCommand> decodeCommands(const Bytes& b) {
    if (b.empty()) return {};   // 空 = 无新命令（drain 语义）
    size_t pos = 0;
    if (b.size() < 16 + 4 + 8 + 4) throw std::runtime_error("Net: commands too short");
    std::string magic = net_detail::readMagic(b, pos);
    if (magic != net_detail::expectedMagic(kNetMagicCommands))
        throw std::runtime_error("Net: commands bad magic");
    net_detail::getVersion(b, pos);
    uint64_t count = snap_detail::getU64(b, pos);
    if (count > (b.size() - pos)) throw std::runtime_error("Net: commands corrupted count");
    std::vector<NetCommand> out;
    out.reserve(static_cast<size_t>(count));
    for (uint64_t i = 0; i < count; ++i) out.push_back(net_detail::getCommand(b, pos));
    if (pos + 4 > b.size()) throw std::runtime_error("Net: commands missing crc32");
    uint32_t stored = snap_detail::getU32(b, pos);
    uint32_t computed = crc32(b.data(), pos - 4);
    if (stored != computed) throw std::runtime_error("Net: commands crc32 mismatch");
    return out;
}

// ======================================================================
inline Bytes encodeWorldPool(const NetWorldData& d) {
    Bytes b;
    net_detail::putMagic(b, kNetMagicWorld);
    net_detail::putVersion(b);
    snap_detail::putU64(b, d.schemaVersion);
    snap_detail::putU64(b, d.tickCount);
    snap_detail::putU64(b, d.submissions.size());
    for (const auto& s : d.submissions) net_detail::putSubmission(b, s);
    snap_detail::putU64(b, d.userSections.size());
    for (const auto& u : d.userSections) {
        b.push_back(u.scenario);
        snap_detail::putBytes(b, u.data);
    }
    uint32_t crc = crc32(b.data(), b.size());
    snap_detail::putU32(b, crc);
    return b;
}

inline NetWorldData decodeWorldPool(const Bytes& b) {
    size_t pos = 0;
    if (b.size() < 16 + 4 + 8 + 8 + 8 + 4) throw std::runtime_error("Net: world too short");
    std::string magic = net_detail::readMagic(b, pos);
    if (magic != net_detail::expectedMagic(kNetMagicWorld))
        throw std::runtime_error("Net: world bad magic");
    net_detail::getVersion(b, pos);
    NetWorldData d;
    d.schemaVersion = snap_detail::getU64(b, pos);
    d.tickCount = snap_detail::getU64(b, pos);
    uint64_t nSubs = snap_detail::getU64(b, pos);
    if (nSubs > (b.size() - pos)) throw std::runtime_error("Net: world corrupted submission count");
    d.submissions.reserve(static_cast<size_t>(nSubs));
    for (uint64_t i = 0; i < nSubs; ++i) d.submissions.push_back(net_detail::getSubmission(b, pos));
    uint64_t nUser = snap_detail::getU64(b, pos);
    if (nUser > (b.size() - pos)) throw std::runtime_error("Net: world corrupted user-section count");
    d.userSections.reserve(static_cast<size_t>(nUser));
    for (uint64_t i = 0; i < nUser; ++i) {
        if (pos + 1 > b.size()) throw std::runtime_error("Net: world truncated (scenario)");
        NetUserSection u;
        u.scenario = b[pos++];
        u.data = snap_detail::getBytes(b, pos);
        d.userSections.push_back(std::move(u));
    }
    if (pos + 4 > b.size()) throw std::runtime_error("Net: world missing crc32");
    uint32_t stored = snap_detail::getU32(b, pos);
    uint32_t computed = crc32(b.data(), pos - 4);
    if (stored != computed) throw std::runtime_error("Net: world crc32 mismatch");
    return d;
}
