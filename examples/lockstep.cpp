// lockstep.cpp -- two peers running the same simulation from an exchanged command stream.
//
// What it demonstrates:
//   * identical task definitions on both peers (same order -- the canonical command order depends on it)
//   * the host issues commands; the client receives them through exportCommands()/importCommands()
//   * commands are injected at a TICK BOUNDARY (a command targeting the tick you are currently inside
//     is rejected: it could never run, because that tick is already dispatched)
//   * per-tick world hashes must match -- that is the whole point of a deterministic engine
//   * `deps` ordering: "Report" is declared to run AFTER "Input" within the same tick
//
// Build (MSVC):
//   cl /nologo /std:c++20 /utf-8 /O2 /EHsc /I <lib>/include /I <vcpkg>/installed/x64-windows/include ^
//      <lib>/examples/lockstep.cpp <lib>/src/ThreadPool.cpp /Fe:lockstep.exe
//
// Comments are ASCII on purpose (see minimal.cpp).

#include "TickPool.h"
#include <chrono>
#include <cstdio>
#include <vector>

using namespace std::chrono_literals;
using Pool = TickPool<std::chrono::milliseconds>;

// ---------------------------------------------------------------------------------------------
// The user's world. Only `destruct` may mutate it. Both fields are hash-visible, so an ordering
// mistake (Report running before Input) would immediately show up as a hash mismatch.
// ---------------------------------------------------------------------------------------------
struct World {
    long long total = 0;
    long long reportedTotal = -1;
};

static uint64_t worldHashCb(Pool::Context&, void* userData) {
    auto* w = static_cast<World*>(userData);
    uint64_t h = 1469598103934665603ull;                       // FNV-1a offset basis
    h = (h ^ static_cast<uint64_t>(w->total)) * 1099511628211ull;
    h = (h ^ static_cast<uint64_t>(w->reportedTotal)) * 1099511628211ull;
    return h;
}

// ---------------------------------------------------------------------------------------------
// Identical definitions on both peers. `issueInput` is true only for the host: in lockstep the
// client has no local input of its own, it only replays what the host sends.
// ---------------------------------------------------------------------------------------------
static void defineTasks(Pool& pool, World& world, bool issueInput) {
    pool.defineTask("Input")
        .parallelResult<int>()
        .action<int>("Apply", [](int v) { return v * 3; })      // parallel phase: pure compute
        .construct([&pool, issueInput] {
            if (!issueInput) return;                            // client: commands arrive from the network
            // One command per tick, aimed at the NEXT tick. delay must be >= 1: the current tick is
            // already dispatched, so a command aimed at "now" could never run.
            pool.submit("Input").action("Apply").options({ .delay = 1 }).work(1);
        })
        .destruct([&pool, &world] {
            pool.withParallelResults<int>("Input", [&world](auto& results) {
                for (int v : results) world.total += v;          // merge parallel results into the world
            });
        })
        .options(TaskDesc{});

    // Runs AFTER "Input" in the same tick. The deps lists describe the OTHER task's position:
    // `.before = { "Input" }` reads as "Input comes before me" -> this task runs second.
    pool.defineTask("Report")
        .construct([] {})
        .destruct([&world] { world.reportedTotal = world.total; })
        .options(TaskDesc{ .deps = { .before = { "Input" } } });

    pool.setWorldHash(&worldHashCb, &world);
}

int main() {
    World hostWorld, clientWorld;
    Pool host(5ms), client(5ms);       // 5ms ticks: this demo is about logic, not real-time pacing

    defineTasks(host, hostWorld, /*issueInput=*/true);
    defineTasks(client, clientWorld, /*issueInput=*/false);

    const int TICKS = 6;
    bool allMatch = true;

    for (int tick = 0; tick < TICKS; ++tick) {
        host.run(1);                                         // host advances one tick
        const Bytes commands = host.exportCommands();        // incremental drain: empty if nothing new

        // Inject at the tick boundary -- BEFORE the client runs its own tick. Each command carries
        // its absolute executeTick, so the client applies it in the same tick the host did.
        if (!commands.empty()) client.importCommands(commands);

        client.run(1);

        const uint64_t hh = host.worldHash();
        const uint64_t hc = client.worldHash();
        std::printf("tick %d | commands %5zu B | host %016llx | client %016llx | %s\n",
                    tick, commands.size(),
                    static_cast<unsigned long long>(hh), static_cast<unsigned long long>(hc),
                    hh == hc ? "OK" : "MISMATCH (non-deterministic!)");
        if (hh != hc) allMatch = false;
    }

    std::printf("\nworld: host total=%lld reported=%lld | client total=%lld reported=%lld\n",
                hostWorld.total, hostWorld.reportedTotal, clientWorld.total, clientWorld.reportedTotal);
    std::printf("%s\n", allMatch ? "PASS: peers stayed in sync"
                                 : "FAIL: peers diverged -- see the determinism rules in the README");
    return allMatch ? 0 : 1;
}
