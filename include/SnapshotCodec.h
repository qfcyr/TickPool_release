#pragma once
// SnapshotCodec.h —— 快照格式编解码（二进制 + JSON 同构）

#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <array>
#include <stdexcept>
#include <algorithm>

using Bytes = std::vector<uint8_t>;

// ---------- 兼容策略（决策 9；默认值由 DefaultTickPoolOptions::snapshotCompat 提供） ----------
enum class SnapshotCompatPolicy { Any, Backward, Forward, Strict };

// 框架格式版本（决策 9：由框架自管、随版本递增）
inline constexpr uint32_t TICKPOOL_SNAPSHOT_FORMAT_VERSION = 2;

// ---------- 快照数据（与具体编码后端无关的中间表示） ----------
struct SnapshotSubmission {
    std::string task;
    std::string action;
    uint64_t executeTick = 0;
    uint64_t submitTick = 0;
    Bytes payload;
};

struct SnapshotUserSection {
    uint8_t scenario = 0;   // SnapshotScenario 的底层值
    Bytes data;
};

// JSON 调试兼容字段（旧 toString 格式：timeScale + tasks；二进制格式不写这两项）
struct SnapshotTaskInfo {
    std::string key;
    uint64_t pendingAsync = 0;
};

struct SnapshotData {
    uint64_t schemaVersion = 0;
    uint64_t tickCount = 0;
    double timeScale = 1.0;                 // JSON 调试兼容（二进制不写）
    std::vector<SnapshotTaskInfo> tasks;    // JSON 调试兼容（二进制不写）
    std::vector<SnapshotSubmission> submissions;
    std::vector<SnapshotUserSection> userSections;
    std::vector<Bytes> rollbackFrames;      // 随档保存的回滚帧（每帧=编码后的环帧；v2 起）
};

// ---------- CRC32（IEEE 802.3 多项式 0xEDB88320） ----------

// 两个线程首次并发进入会无同步地读写 table → 数据竞争，可能算出错误 CRC
// （exportCommands/exportWorldState 文档承诺「任意线程/任意时刻可调用」，故该竞态可达）。
constexpr std::array<uint32_t, 256> makeCrc32Table() {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k)
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        t[i] = c;
    }
    return t;
}
inline constexpr std::array<uint32_t, 256> kCrc32Table = makeCrc32Table();

inline uint32_t crc32(const uint8_t* data, size_t size) noexcept {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < size; ++i)
        crc = kCrc32Table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

// ---------- 二进制编码辅助 ----------
namespace snap_detail {

    inline void putU32(Bytes& b, uint32_t v) {
        b.push_back(static_cast<uint8_t>(v & 0xFF));
        b.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        b.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        b.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    }
    inline void putU64(Bytes& b, uint64_t v) {
        for (int i = 0; i < 8; ++i) b.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
    }
    inline uint32_t getU32(const Bytes& b, size_t& pos) {
        if (pos + 4 > b.size()) throw std::runtime_error("Snapshot: truncated (u32)");
        uint32_t v = 0;
        for (int i = 3; i >= 0; --i) v = (v << 8) | b[pos + static_cast<size_t>(i)];
        pos += 4;
        return v;
    }
    inline uint64_t getU64(const Bytes& b, size_t& pos) {
        if (pos + 8 > b.size()) throw std::runtime_error("Snapshot: truncated (u64)");
        uint64_t v = 0;
        for (int i = 7; i >= 0; --i) v = (v << 8) | b[pos + static_cast<size_t>(i)];
        pos += 8;
        return v;
    }
    inline void putStr(Bytes& b, const std::string& s) {
        putU64(b, s.size());
        b.insert(b.end(), reinterpret_cast<const uint8_t*>(s.data()),
            reinterpret_cast<const uint8_t*>(s.data()) + s.size());
    }
    inline std::string getStr(const Bytes& b, size_t& pos) {
        uint64_t n = getU64(b, pos);
        if (pos + n > b.size()) throw std::runtime_error("Snapshot: truncated (str)");
        std::string s(reinterpret_cast<const char*>(b.data() + pos), static_cast<size_t>(n));
        pos += static_cast<size_t>(n);
        return s;
    }
    inline void putBytes(Bytes& b, const Bytes& data) {
        putU64(b, data.size());
        b.insert(b.end(), data.begin(), data.end());
    }
    inline Bytes getBytes(const Bytes& b, size_t& pos) {
        uint64_t n = getU64(b, pos);
        if (pos + n > b.size()) throw std::runtime_error("Snapshot: truncated (bytes)");
        Bytes out(b.begin() + static_cast<std::ptrdiff_t>(pos), b.begin() + static_cast<std::ptrdiff_t>(pos + static_cast<size_t>(n)));
        pos += static_cast<size_t>(n);
        return out;
    }

    // base64（JSON 用户段/负载编码用）
    inline constexpr const char* b64Chars() { return "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"; }

    constexpr std::array<int, 256> makeB64DecodeTable() {
        std::array<int, 256> t{};
        for (int i = 0; i < 256; ++i) t[static_cast<size_t>(i)] = -1;
        for (int i = 0; i < 64; ++i) t[static_cast<unsigned char>(b64Chars()[i])] = i;
        return t;
    }
    inline constexpr std::array<int, 256> kB64DecodeTable = makeB64DecodeTable();
    inline std::string toBase64(const Bytes& data) {
        std::string out;
        out.reserve(((data.size() + 2) / 3) * 4);
        const char* C = b64Chars();
        size_t i = 0;
        while (i + 3 <= data.size()) {
            uint32_t v = (data[i] << 16) | (data[i + 1] << 8) | data[i + 2];
            out.push_back(C[(v >> 18) & 63]); out.push_back(C[(v >> 12) & 63]);
            out.push_back(C[(v >> 6) & 63]); out.push_back(C[v & 63]);
            i += 3;
        }
        if (i + 1 == data.size()) {
            uint32_t v = data[i] << 16;
            out.push_back(C[(v >> 18) & 63]); out.push_back(C[(v >> 12) & 63]); out.push_back('='); out.push_back('=');
        }
        else if (i + 2 == data.size()) {
            uint32_t v = (data[i] << 16) | (data[i + 1] << 8);
            out.push_back(C[(v >> 18) & 63]); out.push_back(C[(v >> 12) & 63]); out.push_back(C[(v >> 6) & 63]); out.push_back('=');
        }
        return out;
    }
    inline Bytes fromBase64(const std::string& s) {
        const auto& table = kB64DecodeTable;
        Bytes out;
        out.reserve(s.size() / 4 * 3);
        uint32_t buf = 0; int bits = 0;
        for (char c : s) {
            if (c == '=' || c == '\n' || c == '\r') continue;
            int v = table[static_cast<unsigned char>(c)];
            if (v < 0) throw std::runtime_error("Snapshot: invalid base64");
            buf = (buf << 6) | static_cast<uint32_t>(v);
            bits += 6;
            if (bits >= 8) {
                bits -= 8;
                out.push_back(static_cast<uint8_t>((buf >> bits) & 0xFF));
            }
        }
        return out;
    }

} // namespace snap_detail

// ---------- 二进制编码 ----------
inline Bytes encodeSnapshot(const SnapshotData& d) {
    Bytes b;
    // magic
    const char* magic = "TICKPOOL_SNAP_V2";
    b.insert(b.end(), magic, magic + 16);
    snap_detail::putU32(b, TICKPOOL_SNAPSHOT_FORMAT_VERSION);
    snap_detail::putU64(b, d.schemaVersion);
    snap_detail::putU32(b, 0);   // flags（保留）
    snap_detail::putU64(b, d.tickCount);

    // pending submissions（规范化排序 → 字节稳定）
    std::vector<SnapshotSubmission> subs = d.submissions;
    std::sort(subs.begin(), subs.end(), [](const SnapshotSubmission& a, const SnapshotSubmission& b) {
        if (a.executeTick != b.executeTick) return a.executeTick < b.executeTick;
        if (a.task != b.task) return a.task < b.task;
        return a.action < b.action;
    });
    snap_detail::putU64(b, subs.size());
    for (const auto& s : subs) {
        snap_detail::putStr(b, s.task);
        snap_detail::putStr(b, s.action);
        snap_detail::putU64(b, s.executeTick);
        snap_detail::putBytes(b, s.payload);
    }

    // user sections
    snap_detail::putU64(b, d.userSections.size());
    for (const auto& u : d.userSections) {
        b.push_back(u.scenario);
        snap_detail::putBytes(b, u.data);
    }

    // 随档保存的回滚帧段
    snap_detail::putU64(b, d.rollbackFrames.size());
    for (const auto& f : d.rollbackFrames) {
        snap_detail::putBytes(b, f);
    }

    // crc32 footer
    uint32_t crc = crc32(b.data(), b.size());
    snap_detail::putU32(b, crc);
    return b;
}

inline void decodeSnapshot(const Bytes& b, SnapshotData& out, SnapshotCompatPolicy policy) {
    size_t pos = 0;
    if (b.size() < 16 + 4 + 8 + 4 + 8 + 4) throw std::runtime_error("Snapshot: too short");
    std::string magic(b.begin(), b.begin() + 16);
    if (magic != "TICKPOOL_SNAP_V2") throw std::runtime_error("Snapshot: bad magic");
    pos += 16;
    uint32_t snapVer = snap_detail::getU32(b, pos);
    const uint32_t curVer = TICKPOOL_SNAPSHOT_FORMAT_VERSION;
    switch (policy) {
    case SnapshotCompatPolicy::Strict:  if (snapVer != curVer) throw std::runtime_error("Snapshot: version mismatch (Strict) V_snap=" + std::to_string(snapVer) + " V_cur=" + std::to_string(curVer)); break;
    case SnapshotCompatPolicy::Backward:if (snapVer > curVer) throw std::runtime_error("Snapshot: newer than current (Backward) V_snap=" + std::to_string(snapVer) + " V_cur=" + std::to_string(curVer)); break;
    case SnapshotCompatPolicy::Forward: if (snapVer < curVer) throw std::runtime_error("Snapshot: older than current (Forward) V_snap=" + std::to_string(snapVer) + " V_cur=" + std::to_string(curVer)); break;
    case SnapshotCompatPolicy::Any:     break;
    }
    out.schemaVersion = snap_detail::getU64(b, pos);
    snap_detail::getU32(b, pos);   // flags（忽略未知位）
    out.tickCount = snap_detail::getU64(b, pos);

    uint64_t nSubs = snap_detail::getU64(b, pos);
    if (nSubs > (b.size() - pos)) throw std::runtime_error("Snapshot: corrupted submission count");
    out.submissions.clear();
    out.submissions.reserve(static_cast<size_t>(nSubs));
    for (uint64_t i = 0; i < nSubs; ++i) {
        SnapshotSubmission s;
        s.task = snap_detail::getStr(b, pos);
        s.action = snap_detail::getStr(b, pos);
        s.executeTick = snap_detail::getU64(b, pos);
        s.payload = snap_detail::getBytes(b, pos);
        out.submissions.push_back(std::move(s));
    }

    uint64_t nUser = snap_detail::getU64(b, pos);
    if (nUser > (b.size() - pos)) throw std::runtime_error("Snapshot: corrupted user-section count");
    out.userSections.clear();
    out.userSections.reserve(static_cast<size_t>(nUser));
    for (uint64_t i = 0; i < nUser; ++i) {
        if (pos + 1 > b.size()) throw std::runtime_error("Snapshot: truncated (scenario)");
        SnapshotUserSection u;
        u.scenario = b[pos++];
        u.data = snap_detail::getBytes(b, pos);
        out.userSections.push_back(std::move(u));
    }

    // 回滚帧段（v1 无此段）
    out.rollbackFrames.clear();
    if (snapVer >= 2) {
        uint64_t nFrames = snap_detail::getU64(b, pos);
        if (nFrames > (b.size() - pos)) throw std::runtime_error("Snapshot: corrupted rollback-frame count");
        out.rollbackFrames.reserve(static_cast<size_t>(nFrames));
        for (uint64_t i = 0; i < nFrames; ++i) {
            out.rollbackFrames.push_back(snap_detail::getBytes(b, pos));
        }
    }

    // crc32 校验
    if (pos + 4 > b.size()) throw std::runtime_error("Snapshot: missing crc32");
    uint32_t stored = snap_detail::getU32(b, pos);
    uint32_t computed = crc32(b.data(), pos - 4);
    if (stored != computed) throw std::runtime_error("Snapshot: crc32 mismatch");
}

#if defined(TICKPOOL_ENABLE_JSON)
// ---------- JSON 编码 ----------
// 输出字段顺序：tickCount, timeScale, tasks（旧格式），submissions/userSections 仅非空时输出。
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

inline std::string encodeSnapshotJson(const SnapshotData& d) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();
    doc.AddMember("tickCount", d.tickCount, alloc);
    doc.AddMember("timeScale", d.timeScale, alloc);
    rapidjson::Value tasks(rapidjson::kArrayType);
    for (const auto& t : d.tasks) {
        rapidjson::Value o(rapidjson::kObjectType);
        o.AddMember("key", rapidjson::Value(t.key.c_str(), alloc), alloc);
        o.AddMember("pendingAsync", t.pendingAsync, alloc);
        tasks.PushBack(o, alloc);
    }
    doc.AddMember("tasks", tasks, alloc);

    std::vector<SnapshotSubmission> subs = d.submissions;
    std::sort(subs.begin(), subs.end(), [](const SnapshotSubmission& a, const SnapshotSubmission& b) {
        if (a.executeTick != b.executeTick) return a.executeTick < b.executeTick;
        if (a.task != b.task) return a.task < b.task;
        return a.action < b.action;
    });
    if (!subs.empty()) {
        rapidjson::Value subArr(rapidjson::kArrayType);
        for (const auto& s : subs) {
            rapidjson::Value o(rapidjson::kObjectType);
            o.AddMember("task", rapidjson::Value(s.task.c_str(), alloc), alloc);
            o.AddMember("action", rapidjson::Value(s.action.c_str(), alloc), alloc);
            o.AddMember("executeTick", s.executeTick, alloc);
            o.AddMember("payload", rapidjson::Value(snap_detail::toBase64(s.payload).c_str(), alloc), alloc);
            subArr.PushBack(o, alloc);
        }
        doc.AddMember("submissions", subArr, alloc);
    }
    if (!d.userSections.empty()) {
        rapidjson::Value users(rapidjson::kArrayType);
        for (const auto& u : d.userSections) {
            rapidjson::Value o(rapidjson::kObjectType);
            o.AddMember("scenario", u.scenario, alloc);
            o.AddMember("data", rapidjson::Value(snap_detail::toBase64(u.data).c_str(), alloc), alloc);
            users.PushBack(o, alloc);
        }
        doc.AddMember("userSections", users, alloc);
    }

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    return buffer.GetString();
}

inline void decodeSnapshotJson(const std::string& json, SnapshotData& out) {
    rapidjson::Document doc;
    if (doc.Parse(json.c_str()).HasParseError()) throw std::runtime_error("Snapshot: bad JSON");
    out.tickCount = doc.HasMember("tickCount") ? doc["tickCount"].GetUint64() : 0;
    out.timeScale = doc.HasMember("timeScale") ? doc["timeScale"].GetDouble() : 1.0;
    out.tasks.clear();
    if (doc.HasMember("tasks") && doc["tasks"].IsArray()) {
        for (auto& t : doc["tasks"].GetArray()) {
            SnapshotTaskInfo info;
            info.key = t["key"].GetString();
            if (t.HasMember("pendingAsync")) info.pendingAsync = t["pendingAsync"].GetUint64();
            out.tasks.push_back(std::move(info));
        }
    }
    out.submissions.clear();
    if (doc.HasMember("submissions") && doc["submissions"].IsArray()) {
        for (auto& s : doc["submissions"].GetArray()) {
            SnapshotSubmission sub;
            sub.task = s["task"].GetString();
            sub.action = s["action"].GetString();
            sub.executeTick = s["executeTick"].GetUint64();
            if (s.HasMember("payload")) sub.payload = snap_detail::fromBase64(s["payload"].GetString());
            out.submissions.push_back(std::move(sub));
        }
    }
    out.userSections.clear();
    if (doc.HasMember("userSections") && doc["userSections"].IsArray()) {
        for (auto& u : doc["userSections"].GetArray()) {
            SnapshotUserSection sec;
            sec.scenario = static_cast<uint8_t>(u["scenario"].GetUint());
            if (u.HasMember("data")) sec.data = snap_detail::fromBase64(u["data"].GetString());
            out.userSections.push_back(std::move(sec));
        }
    }
}
#endif // TICKPOOL_ENABLE_JSON
