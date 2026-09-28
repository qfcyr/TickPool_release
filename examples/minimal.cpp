// minimal.cpp -- smallest end-to-end TickPool usage sample.
//
// Comments are ASCII on purpose: sources are compiled with /utf-8, and non-ASCII in a file that
// someone copies into a differently-configured project is a needless trap.
//
// Build (MSVC):
//   cl /nologo /std:c++20 /utf-8 /O2 /EHsc /I <lib>/include <lib>/examples/minimal.cpp ^
//      <lib>/src/ThreadPool.cpp /Fe:minimal.exe

#include "TickPool.h"
#include <chrono>
#include <cstdio>

using namespace std::chrono_literals;
using Pool = TickPool<std::chrono::milliseconds>;

int main() {
    Pool pool(50ms);                  // owns a work-stealing thread pool; fixed 50ms tick

    int  world       = 0;             // pretend game world
    long resultsSeen = 0;

    // ---------------------------------------------------------------------------------------
    // ORDERING IDIOM -- read this before copying the code below.
    //
    //   deps.after  = { X }   means  "X comes AFTER me"  ->  I run FIRST.
    //   deps.before = { Z }   means  "Z comes BEFORE me" ->  Z runs first.
    //
    // The names describe the OTHER task's position, not this one's, which is easy to get backwards.
    // Both lists are honoured equally by the scheduler, so use whichever reads better at the call
    // site. A constraint is only ever violated by declaring *conflicting* orders for the same pair
    // (a cycle), which the compiler rejects instead of scheduling it.
    //
    // To make Init run before Physics, declare it ON Init (not on Physics):  .after = { "Physics" }
    // The same statement written on the other side is  Physics.before = { "Init" }.
    // ---------------------------------------------------------------------------------------

    // wave 0: Init runs before Physics; Load (async) also starts in wave 0
    pool.defineTask("Init")
        .construct([&world] { world = 100; })
        .destruct([] {})
        .options(TaskDesc{ .deps = { .after = { "Physics" } } });      // "Physics comes after me"

    // wave 1: the compute task -- fans out into 8 parallel subtasks per tick
    pool.defineTask("Physics")
        .parallelResult<int>()
        .action<int>("Step", [](int i) {
            return 100 + i;               // parallel phase: pure compute, no shared writes
        })
        .construct([&pool] {
            for (int i = 0; i < 8; ++i)
                pool.submit("Physics").action("Step").options({ .delay = 1 }).work(i);
        })
        .destruct([&pool, &resultsSeen, &world] {
            long sum = 0;                 // the ONLY phase allowed to mutate the world:
            pool.withParallelResults<int>("Physics", [&](auto& results) {
                resultsSeen += static_cast<long>(results.size());
                for (int v : results) sum += v;
            });
            world = static_cast<int>(sum);
        })
        .options(TaskDesc{ .deps = { .after = { "Report" } } });       // "Report comes after me"

    // wave 2: runs after Physics
    pool.defineTask("Report")
        .construct([] {})
        .destruct([] {})
        .options(TaskDesc{});

    // wave 0: asynchronous work -- executes immediately on the CALLING thread, result returns later
    pool.defineTask("Load")
        .asyncResult<int>()
        .construct([&pool] { pool.submit("Load").work([] { return 7; }); })
        .destruct([&pool] { pool.withAsyncResults<int>("Load", [](auto&) {}); })
        .options(TaskDesc{ .deps = { .after = { "Physics" } } });      // "Physics comes after me"

    // Optional error hook: plain function pointer + userData pointer (no capturing lambdas).
    pool.onTaskException([](Pool::Context&, const std::exception& e, void*) {
        std::fprintf(stderr, "[task exception] %s\n", e.what());
    });

    pool.run(3);                      // run 3 ticks synchronously on THIS thread

    std::printf("tickCount   = %zu\n", pool.tickCount());
    std::printf("world       = %d\n", world);
    std::printf("resultsSeen = %ld\n", resultsSeen);

    // Inspect the derived DAG as Mermaid -- handy in code reviews and bug reports.
    // (The trailing %% warning the generator can emit means an edge contradicts the wave order;
    //  with ordering expressed via `after` as above, there is none.)
    std::printf("\n--- toMermaid() ---\n%s", pool.toMermaid(true).c_str());
    return 0;
}
