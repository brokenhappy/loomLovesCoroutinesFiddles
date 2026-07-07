package benchmarks

import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import loom.loomToCoroutines
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Worst case for LoomCompatibleCoroutineDispatcher's extra work (ScopedValue capture/install on
 * every dispatch, a fresh virtual thread per dispatch, and the AtomicInteger interrupt-on-cancel
 * handshake): many concurrently dispatching coroutines each doing a tight yield() loop, since
 * yield() always forces a real redispatch. bareVirtualThreadDispatcher is the same shape with none
 * of that extra work, as a baseline.
 *
 * Results on 2023 14" Apple M2 Max 64GB:
 *
 * Benchmark                                                Mode  Cnt     Score     Error  Units
 * LoomBridgeDispatchBenchmark.bareVirtualThreadDispatcher  avgt   10  4838,436 ± 342,741  us/op
 * LoomBridgeDispatchBenchmark.loomToCoroutinesWrapper      avgt   10  5192,482 ± 101,973  us/op
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
open class LoomBridgeDispatchBenchmark {
    private val concurrentCoroutines = 30
    private val yieldsPerCoroutine = 1_000

    private lateinit var virtualThreadDispatcher: ExecutorCoroutineDispatcher

    @Setup(Level.Trial)
    fun setUp() {
        virtualThreadDispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
    }

    @TearDown(Level.Trial)
    fun tearDown() {
        virtualThreadDispatcher.close()
    }

    @Benchmark
    open fun bareVirtualThreadDispatcher() = runBlocking(virtualThreadDispatcher) {
        repeat(concurrentCoroutines) {
            launch { repeat(yieldsPerCoroutine) { yield() } }
        }
    }

    @Benchmark
    open fun loomToCoroutinesWrapper() = loomToCoroutines {
        repeat(concurrentCoroutines) {
            launch { repeat(yieldsPerCoroutine) { yield() } }
        }
    }
}
