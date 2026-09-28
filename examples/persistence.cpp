// persistence.cpp -- snapshots, rollback and world (de)serialisation.
//
// What it demonstrates:
//   * a snapshot scenario hook: your world <-> opaque Bytes (the library never inspects them)
//   * save -> keep running -> restore == the run never happened (verified through worldHash())
//   * a rollback buffer: run -> roll back N ticks -> replay == the original run, tick by tick
//   * disk persistence (saveSnapshotToFile / loadSnapshotFromFile) including the rollback frames
//
// Build (MSVC):
//   cl /nologo /std:c++20 /utf-8 /O2 /EHsc /I <lib>/include /I <vcpkg>/installed/x64-windows/include ^
//      <lib>/examples/persistence.cpp <lib>/src/ThreadPool.cpp /Fe:persistence.exe
//
// Comments are ASCII on purpose (see minimal.cpp).

#include "TickPool.h"
#include <chrono>
#include <cstdio>
#include <vector>

using namespace std::chrono_literals;
using Pool = TickPool<std::chrono::milliseconds>;

struct World {
    std::vector<int> marks;
};

// ---- scenario hooks: Bytes is an opaque carrier, the format is entirely yours -------------------
static Bytes worldSave(Pool::Context&, void* userData) {
    auto* w = static_cast<World*>(userData);
    Bytes out;
    out.reserve(w->marks.size() * sizeof(int));
    for (int v : w->marks) {
        const auto u = static_cast<uint32_t>(v);
        out.push_back(static_cast<uint8_t>(u & 0xFF));
        out.push_back(static_cast<uint8_t>((u >> 8) & 0xFF));
        out.push_back(static_cast<uint8_t>((u >> 16) & 0xFF));
        out.push_back(static_cast<uint8_t>((u >> 24) & 0xFF));
    }
    return out;
}

static void worldLoad(Pool::Context&, const Bytes& bytes, void* userData) {
    auto* w = static_cast<World*>(userData);
    w->marks.clear();
    for (size_t i = 0; i + 4 <= bytes.size(); i += 4) {
        const uint32_t u = static_cast<uint32_t>(bytes[i]) | (static_cast<uint32_t>(bytes[i + 1]) << 8)
                         | (static_cast<uint32_t>(bytes[i + 2]) << 16) | (static_cast<uint32_t>(bytes[i + 3]) << 24);
        w->marks.push_back(static_cast<int>(u));
    }
}

static uint64_t worldHashCb(Pool::Context&, void* userData) {
    auto* w = static_cast<World*>(userData);
    uint64_t h = 1469598103934665603ull;
    for (int v : w->marks) h = (h ^ static_cast<uint64_t>(v)) * 1099511628211ull;
    return h;
}

int main() {
    World world;
    Pool pool(1ms);
    pool.setWorldCodec(&worldSave, &worldLoad, &world);   // one codec for all four scenarios
    pool.setWorldHash(&worldHashCb, &world);
    pool.setRollbackBuffer(8, /*everyTicks=*/1);          // keep the last 8 tick-boundary frames

    // The one task in this demo: every tick it stamps the tick index into the world.
    pool.defineTask("Marker")
        .construct([] {})
        .destruct([&pool, &world] { world.marks.push_back(static_cast<int>(pool.tickCount())); })
        .options(TaskDesc{});

    pool.run(3);
    const uint64_t hashAtSave = pool.worldHash();
    std::printf("[1] after 3 ticks: marks=%zu hash=%016llx\n",
                world.marks.size(), static_cast<unsigned long long>(hashAtSave));

    const Bytes snapshot = pool.exportSnapshot();

    // Let the world drift, then restore. The snapshot carries BOTH the pool state (pending
    // submissions, tick count, ...) and the user world section produced by our hook.
    pool.run(2);
    std::printf("[2] after 2 more ticks: marks=%zu (drifted)\n", world.marks.size());
    pool.importSnapshot(snapshot);
    const uint64_t hashAfterLoad = pool.worldHash();
    std::printf("[3] restored: marks=%zu hash=%016llx -> %s\n", world.marks.size(),
                static_cast<unsigned long long>(hashAfterLoad),
                hashAfterLoad == hashAtSave ? "identical (save/restore is lossless)"
                                            : "DIFFERENT (bug!)");

    // ---- rollback: record hashes, roll back, replay, compare ------------------------------------
    // Each recording run advances one tick, so hashes[i] is the hash at tickCount == base + i + 1.
    pool.run(5);
    std::vector<uint64_t> hashes;
    for (int i = 0; i < 3; ++i) { pool.run(1); hashes.push_back(pool.worldHash()); }

    if (!pool.rollbackTo(2)) {
        std::printf("[4] rollbackTo failed (buffer disabled?)\n");
        return 1;
    }
    // rollbackTo(2) rewinds to the state 2 ticks ago -- i.e. exactly the point where hashes[0] was taken.
    const bool rewoundExactly = (pool.worldHash() == hashes[0]);

    std::vector<uint64_t> replayed;
    for (int i = 0; i < 3; ++i) { pool.run(1); replayed.push_back(pool.worldHash()); }
    // Replaying therefore reproduces hashes[1] and hashes[2] (the third tick is new history).
    const bool replayMatches = (replayed[0] == hashes[1] && replayed[1] == hashes[2]);
    std::printf("[4] rollback to 2 ticks ago: rewound=%s | replay of the next 2 ticks %s\n",
                rewoundExactly ? "exact" : "MISMATCH",
                replayMatches ? "reproduced the original run" : "DIVERGED (bug!)");

    // ---- disk persistence (includes the rollback frames by default) ------------------------------
    const char* path = "tickpool_demo_snapshot.bin";
    const bool saved = pool.saveSnapshotToFile(path, /*includeRollbackInfo=*/true);
    World probe{ { 999 } };
    Pool other(1ms);
    other.setWorldCodec(&worldSave, &worldLoad, &probe);
    other.defineTask("Marker").construct([] {}).destruct([] {}).options(TaskDesc{});  // same definitions
    const bool loaded = other.loadSnapshotFromFile(path);
    std::printf("[5] disk: save=%s load=%s world=%zu tick=%zu\n",
                saved ? "ok" : "FAILED", loaded ? "ok" : "FAILED",
                probe.marks.size(), other.tickCount());
    std::remove(path);

    const bool ok = (hashAfterLoad == hashAtSave) && rewoundExactly && replayMatches && saved && loaded;
    std::printf("\n%s\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
