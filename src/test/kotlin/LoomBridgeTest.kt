import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.StructuredTaskScope
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

private inline fun <reified T : Throwable> Throwable.chainContains(): Boolean =
    generateSequence(this) { it.cause }.any { it is T }

class LoomBridgeTest {

    // ------------------------------------------------------------------
    // loomToCoroutines: bridging FROM blocking Loom code INTO coroutines
    // ------------------------------------------------------------------

    @Test
    fun `returns the block's result`() {
        assertEquals(42, loomToCoroutines { 42 })
    }

    @Test
    fun `propagates an exception thrown directly in the block`() {
        class Boom : RuntimeException()
        assertFailsWith<Boom> {
            loomToCoroutines { throw Boom() }
        }
    }

    @Test
    fun `propagates an exception thrown by a suspending child, after an actual suspension`() {
        class Boom : RuntimeException()
        assertFailsWith<Boom> {
            loomToCoroutines {
                yield() // force a real suspend+resume before throwing
                throw Boom()
            }
        }
    }

    @Test
    fun `interrupting the calling thread cancels the coroutine and rethrows InterruptedException with the original cause suppressed`() {
        class Boom : RuntimeException()
        val started = CountDownLatch(1)
        val caught = AtomicReference<Throwable?>(null)

        val callerThread = Thread {
            try {
                loomToCoroutines {
                    started.countDown()
                    try {
                        delay(60.seconds)
                    } catch (e: CancellationException) {
                        throw Boom()
                    }
                }
            } catch (e: InterruptedException) {
                caught.set(e)
            }
        }
        callerThread.start()
        assertTrue(started.await(5, TimeUnit.SECONDS))
        callerThread.interrupt()
        callerThread.join(5_000)

        assertTrue(!callerThread.isAlive)
        val e = caught.get()
        assertIs<InterruptedException>(e)
        assertEquals(1, e.suppressed.size)
        assertIs<Boom>(e.suppressed[0])
    }

    // ------------------------------------------------------------------
    // coroutinesToLoom: bridging FROM a coroutine back INTO blocking Loom code
    // ------------------------------------------------------------------

    @Test
    fun `coroutinesToLoom returns the block's result`() {
        val result = loomToCoroutines { coroutinesToLoom { 6 * 7 } }
        assertEquals(42, result)
    }

    @Test
    fun `coroutinesToLoom runs the block on a virtual thread`() {
        val isVirtual = loomToCoroutines { coroutinesToLoom { Thread.currentThread().isVirtual } }
        assertTrue(isVirtual)
    }

    @Test
    fun `coroutinesToLoom exposes the ScopedValues captured at the enclosing loomToCoroutines`() {
        val requestId = ScopedValue.newInstance<String>()
        val seen = ScopedValue.where(requestId, "outer-caller").call<String, RuntimeException> {
            loomToCoroutines { coroutinesToLoom { requestId.get() } }
        }
        assertEquals("outer-caller", seen)
    }

    @Test
    fun `coroutinesToLoom unwraps a raw StructuredTaskScope ExecutionException by default`() {
        class Boom : RuntimeException("boom")
        assertFailsWith<Boom> {
            loomToCoroutines {
                coroutinesToLoom {
                    StructuredTaskScope.open<Unit>().use { scope ->
                        scope.fork(Callable<Unit> { throw Boom() })
                        scope.join()
                    }
                }
            }
        }
    }

    @Test
    fun `coroutinesToLoom with unwrapExecutionException false preserves the ExecutionException`() {
        class Boom : RuntimeException("boom")
        val e = assertFailsWith<ExecutionException> {
            loomToCoroutines {
                coroutinesToLoom(unwrapExecutionException = false) {
                    StructuredTaskScope.open<Unit>().use { scope ->
                        scope.fork(Callable<Unit> { throw Boom() })
                        scope.join()
                    }
                }
            }
        }
        assertTrue(e.chainContains<Boom>())
    }

    @Test
    fun `cancelling the coroutine while coroutinesToLoom's block runs interrupts it and surfaces as CancellationException`() {
        val started = CountDownLatch(1)
        val interrupted = CountDownLatch(1)
        loomToCoroutines {
            val job = launch {
                try {
                    coroutinesToLoom {
                        started.countDown()
                        Thread.sleep(60_000)
                    }
                } catch (e: CancellationException) {
                    interrupted.countDown()
                }
            }
            assertTrue(started.await(5, TimeUnit.SECONDS))
            job.cancel()
        }
        assertTrue(interrupted.await(5, TimeUnit.SECONDS))
    }

    // ------------------------------------------------------------------
    // CoroutineContext propagation: only guaranteed across an explicit coroutinesToLoom crossing
    // ------------------------------------------------------------------

    @Test
    fun `a nested loomToCoroutines inherits CoroutineContext elements from the enclosing coroutine, via coroutinesToLoom`() {
        val seen = loomToCoroutines {
            withContext(CoroutineName("outer-name")) {
                coroutinesToLoom {
                    loomToCoroutines { coroutineContext[CoroutineName]?.name }
                }
            }
        }
        assertEquals("outer-name", seen)
    }

    @Test
    fun `calling loomToCoroutines directly from suspend code, skipping coroutinesToLoom, does not see the enclosing context`() {
        // contextAsScopedValue (the ambient-context mirror) is only bound inside coroutinesToLoom's
        // block. Calling loomToCoroutines straight from suspend code never crosses that bridge, so
        // it can't see it - the narrower, but much simpler and more reliable, contract this design
        // trades for versus threading everything through a custom dispatcher.
        val seen = loomToCoroutines {
            withContext(CoroutineName("outer-name")) {
                loomToCoroutines { coroutineContext[CoroutineName]?.name }
            }
        }
        assertEquals(null, seen)
    }

    @Test
    fun `CoroutineContext accumulates through multiple levels of nested crossings`() {
        data class Seen(val outer: String?, val inner: String?)

        val seen = loomToCoroutines {
            withContext(CoroutineName("outer")) {
                coroutinesToLoom {
                    loomToCoroutines {
                        withContext(CoroutineName("inner")) {
                            coroutinesToLoom {
                                loomToCoroutines {
                                    Seen(
                                        outer = coroutineContext[CoroutineName]?.name,
                                        inner = coroutineContext[CoroutineName]?.name,
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
        // the innermost withContext wins for the (single-valued) CoroutineName key, but it proves
        // the chain of nested crossings kept threading the ambient context through.
        assertEquals(Seen(outer = "inner", inner = "inner"), seen)
    }

    @Test
    fun `independent top-level loomToCoroutines calls on different threads never see each other's CoroutineContext`() {
        val callers = 100
        val mismatches = AtomicInteger(0)

        val threads = List(callers) { i ->
            Thread {
                val expected = "caller-$i"
                val seen = loomToCoroutines {
                    withContext(CoroutineName(expected)) {
                        coroutinesToLoom {
                            loomToCoroutines { coroutineContext[CoroutineName]?.name }
                        }
                    }
                }
                if (seen != expected) mismatches.incrementAndGet()
            }
        }
        threads.forEach { it.start() }
        threads.forEach { it.join(20_000) }
        assertTrue(threads.none { it.isAlive })
        assertEquals(0, mismatches.get())
    }

    @Test
    fun `a coroutine's own Job is not leaked into a nested loomToCoroutines's context`() {
        // each loomToCoroutines launches its own root Job; inheriting the *outer* Job would make
        // the inner one a child of a coroutine that has already returned by the time it starts.
        var outerJob: kotlinx.coroutines.Job? = null
        var innerJob: kotlinx.coroutines.Job? = null
        loomToCoroutines {
            outerJob = coroutineContext[kotlinx.coroutines.Job]
            coroutinesToLoom {
                loomToCoroutines {
                    innerJob = coroutineContext[kotlinx.coroutines.Job]
                }
            }
        }
        assertTrue(outerJob !== innerJob)
    }

    // ------------------------------------------------------------------
    // Scalability: virtual threads must not exhaust like a platform pool would.
    // Interrupt-on-cancel is now only guaranteed for blocking calls wrapped in coroutinesToLoom.
    // ------------------------------------------------------------------

    @Test
    fun `thousands of concurrently blocked sleepers all get interrupted promptly when cancelled`() {
        val sleeperCount = 5_000
        val started = CountDownLatch(sleeperCount)
        val interrupted = CountDownLatch(sleeperCount)

        val elapsed = measureTime {
            loomToCoroutines {
                val jobs = List(sleeperCount) {
                    launch {
                        started.countDown()
                        try {
                            coroutinesToLoom { Thread.sleep(60_000) }
                        } catch (e: CancellationException) {
                            interrupted.countDown()
                        }
                    }
                }
                assertTrue(started.await(30, TimeUnit.SECONDS))
                // "the last coroutine cancels it all"
                launch {
                    jobs.forEach { it.cancel() }
                }
            }
        }

        assertTrue(interrupted.await(30, TimeUnit.SECONDS))
        assertTrue(
            elapsed.inWholeSeconds < 30,
            "expected prompt interruption of $sleeperCount sleepers, took $elapsed",
        )
    }

    // ------------------------------------------------------------------
    // Java structured concurrency -> Kotlin structured concurrency
    // ------------------------------------------------------------------

    @Test
    fun `a loomToCoroutines forked as a Java subtask returns its value normally`() {
        val result = structuredTaskScope {
            fork { loomToCoroutines { 7 * 6 } }
        }.get()
        assertEquals(42, result)
    }

    @Test
    fun `ScopedValues bound on the Java side are visible inside coroutinesToLoom within a forked loomToCoroutines`() {
        val requestId = ScopedValue.newInstance<String>()
        val result = ScopedValue.where(requestId, "java-root").call<String, RuntimeException> {
            structuredTaskScope {
                fork { loomToCoroutines { coroutinesToLoom { requestId.get() } } }
            }.get()
        }
        assertEquals("java-root", result)
    }

    @Test
    fun `a sibling subtask's failure cancels a nested loomToCoroutines, and structuredTaskScope surfaces the original failure unwrapped`() {
        class Boom : RuntimeException("boom")

        val innerStarted = CountDownLatch(1)
        val innerCancelled = CountDownLatch(1)

        assertFailsWith<Boom> {
            structuredTaskScope {
                fork {
                    loomToCoroutines {
                        innerStarted.countDown()
                        try {
                            delay(60.seconds)
                        } catch (e: CancellationException) {
                            innerCancelled.countDown()
                            throw e
                        }
                    }
                }
                fork {
                    innerStarted.await(5, TimeUnit.SECONDS)
                    throw Boom()
                }
            }
        }

        assertTrue(innerCancelled.await(5, TimeUnit.SECONDS))
    }

    // ------------------------------------------------------------------
    // Kotlin structured concurrency -> Java structured concurrency
    // ------------------------------------------------------------------

    @Test
    fun `a coroutine using coroutinesToLoom to fork a Java StructuredTaskScope propagates its subtask's result back`() {
        val result = loomToCoroutines {
            coroutinesToLoom {
                structuredTaskScope { fork { 6 * 7 } }.get()
            }
        }
        assertEquals(42, result)
    }

    @Test
    fun `an exception from a forked Java subtask surfaces inside the coroutine via coroutinesToLoom, unwrapped`() {
        class Boom : RuntimeException("boom")
        assertFailsWith<Boom> {
            loomToCoroutines {
                coroutinesToLoom {
                    structuredTaskScope {
                        fork { throw Boom() }
                    }
                }
            }
        }
    }

    @Test
    fun `cancelling a coroutine blocked in coroutinesToLoom running a nested structuredTaskScope interrupts it and cleans up the forked subtask`() {
        val subtaskStarted = CountDownLatch(1)
        val subtaskInterrupted = CountDownLatch(1)
        val coroutineSawCancellation = CountDownLatch(1)

        loomToCoroutines {
            val job = launch {
                try {
                    coroutinesToLoom {
                        structuredTaskScope {
                            fork {
                                subtaskStarted.countDown()
                                try {
                                    Thread.sleep(60_000)
                                } catch (e: InterruptedException) {
                                    subtaskInterrupted.countDown()
                                }
                            }
                        }
                    }
                } catch (e: CancellationException) {
                    coroutineSawCancellation.countDown()
                }
            }
            assertTrue(subtaskStarted.await(5, TimeUnit.SECONDS))
            job.cancel()
        }

        assertTrue(coroutineSawCancellation.await(5, TimeUnit.SECONDS))
        assertTrue(subtaskInterrupted.await(5, TimeUnit.SECONDS))
    }

    // ------------------------------------------------------------------
    // Isolation between concurrent loomToCoroutines calls
    // ------------------------------------------------------------------

    @Test
    fun `concurrent loomToCoroutines calls with different ScopedValues never see each other's values`() {
        val requestId = ScopedValue.newInstance<String>()
        val callers = 200
        val mismatches = AtomicInteger(0)

        val threads = List(callers) { i ->
            Thread {
                val expected = "caller-$i"
                val seen = ScopedValue.where(requestId, expected).call<String, RuntimeException> {
                    loomToCoroutines { coroutinesToLoom { requestId.get() } }
                }
                if (seen != expected) mismatches.incrementAndGet()
            }
        }
        threads.forEach { it.start() }
        threads.forEach { it.join(20_000) }
        assertTrue(threads.none { it.isAlive })
        assertEquals(0, mismatches.get())
    }
}
