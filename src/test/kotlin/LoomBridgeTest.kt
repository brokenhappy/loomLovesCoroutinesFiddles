import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import loom.loomToCoroutines
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

class LoomBridgeTest {

    // ------------------------------------------------------------------
    // Basics: return values, exceptions, ScopedValues, thread interruption
    // ------------------------------------------------------------------

    @Test
    fun `returns the block's result`() {
        assertEquals(42, loomToCoroutines { 42 })
    }

    @Test
    fun `propagates an exception thrown directly in the block`() {
        class Boom : RuntimeException()
        val e = assertFailsWith<Boom> {
            loomToCoroutines { throw Boom() }
        }
        assertIs<Boom>(e)
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
    fun `sees the ScopedValues bound on the calling thread`() {
        val requestId = ScopedValue.newInstance<String>()
        val seen = ScopedValue.where(requestId, "outer-caller").call<String, RuntimeException> {
            loomToCoroutines { requestId.get() }
        }
        assertEquals("outer-caller", seen)
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
    // Delay support (formerly tested against the dispatcher directly; that class is now a
    // private implementation detail of runBlockingV2, so these go through the public API instead)
    // ------------------------------------------------------------------

    @Test
    fun `ScopedValues are still bound after a delay() suspension`() {
        val requestId = ScopedValue.newInstance<String>()
        val seen = ScopedValue.where(requestId, "after-delay").call<String, RuntimeException> {
            loomToCoroutines {
                delay(50.milliseconds)
                requestId.get()
            }
        }
        assertEquals("after-delay", seen)
    }

    @Test
    fun `execution resumes on a virtual thread after a delay(), not the global default delay thread`() {
        val isVirtual = loomToCoroutines {
            delay(50.milliseconds)
            Thread.currentThread().isVirtual
        }
        assertTrue(isVirtual)
    }

    @Test
    fun `cancelling a coroutine suspended in delay() completes via normal cancellation, not interruption`() {
        val started = CountDownLatch(1)
        var wasCancelled = false
        loomToCoroutines {
            val job = launch {
                started.countDown()
                delay(7.seconds)
            }
            assertTrue(started.await(5, TimeUnit.SECONDS))
            job.cancel()
            job.join()
            wasCancelled = job.isCancelled
        }
        assertTrue(wasCancelled)
    }

    // ------------------------------------------------------------------
    // Scalability: virtual threads must not exhaust like a platform pool would
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
                            Thread.sleep(60_000)
                        } catch (e: InterruptedException) {
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
    fun `a runBlockingV2 forked as a Java subtask returns its value normally`() {
        val result = structuredTaskScope {
            fork { loomToCoroutines { 7 * 6 } }
        }.get()
        assertEquals(42, result)
    }

    @Test
    fun `ScopedValues bound on the Java side are visible inside a runBlockingV2 forked as a subtask`() {
        val requestId = ScopedValue.newInstance<String>()
        val result = ScopedValue.where(requestId, "java-root").call<String, RuntimeException> {
            structuredTaskScope {
                fork { loomToCoroutines { requestId.get() } }
            }.get()
        }
        assertEquals("java-root", result)
    }

    @Test
    fun `a sibling subtask's failure cancels a nested runBlockingV2, and loomScope surfaces the original failure unwrapped`() {
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
    fun `a Job forking a Java StructuredTaskScope propagates its subtask's result back into the coroutine`() {
        val result = loomToCoroutines {
            structuredTaskScope { fork { 6 * 7 } }.get()
        }
        assertEquals(42, result)
    }

    @Test
    fun `an exception from a forked Java subtask surfaces inside the coroutine directly, unwrapped`() {
        class Boom : RuntimeException("boom")
        assertFailsWith<Boom> {
            loomToCoroutines {
                structuredTaskScope {
                    fork { throw Boom() }
                }
            }
        }
    }

    @Test
    fun `cancelling a coroutine blocked in a nested loomScope interrupts it and cleans up the forked subtask`() {
        val subtaskStarted = CountDownLatch(1)
        val subtaskInterrupted = CountDownLatch(1)
        val coroutineSawInterruption = CountDownLatch(1)

        loomToCoroutines {
            val job = launch {
                try {
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
                } catch (e: InterruptedException) {
                    coroutineSawInterruption.countDown()
                }
            }
            assertTrue(subtaskStarted.await(5, TimeUnit.SECONDS))
            job.cancel()
        }

        assertTrue(coroutineSawInterruption.await(5, TimeUnit.SECONDS))
        assertTrue(subtaskInterrupted.await(5, TimeUnit.SECONDS))
    }

    // ------------------------------------------------------------------
    // Isolation between concurrent runBlockingV2 calls sharing the executor
    // ------------------------------------------------------------------

    @Test
    fun `concurrent runBlockingV2 calls with different ScopedValues never see each other's values`() {
        val requestId = ScopedValue.newInstance<String>()
        val callers = 200
        val mismatches = AtomicInteger(0)

        val threads = List(callers) { i ->
            Thread {
                val expected = "caller-$i"
                val seen = ScopedValue.where(requestId, expected).call<String, RuntimeException> {
                    loomToCoroutines {
                        yield() // give other callers a chance to interleave before reading back
                        requestId.get()
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

    // ------------------------------------------------------------------
    // CoroutineContext propagation across nested runBlockingV2 calls
    // ------------------------------------------------------------------

    @Test
    fun `a top-level runBlockingV2 with nothing bound just runs with an empty context`() {
        val name = loomToCoroutines { coroutineContext[CoroutineName]?.name }
        assertEquals(null, name)
    }

    @Test
    fun `a nested runBlockingV2 inherits CoroutineContext elements from the enclosing coroutine`() {
        val seen = loomToCoroutines {
            withContext(CoroutineName("outer-name")) {
                yield() // force a real redispatch - see the "KNOWN LIMITATION" test below for why
                loomToCoroutines { coroutineContext[CoroutineName]?.name }
            }
        }
        assertEquals("outer-name", seen)
    }

    @Test
    fun `KNOWN LIMITATION - a context change isn't visible to a nested runBlockingV2 without an intervening real dispatch`() {
        // contextAsScopedValue is only rebound inside LoomCompatibleCoroutineDispatcher.dispatch().
        // withContext(CoroutineName(...)) doesn't change the dispatcher, so kotlinx.coroutines takes
        // its "undispatched" fast path and never calls dispatch() again - meaning a runBlockingV2
        // called immediately afterward (no suspension in between) still sees the *old* ScopedValue
        // snapshot. A real suspension point (yield(), delay(), a dispatcher switch, ...) in between
        // fixes it, as the test above demonstrates.
        val seen = loomToCoroutines {
            withContext(CoroutineName("outer-name")) {
                loomToCoroutines { coroutineContext[CoroutineName]?.name }
            }
        }
        assertEquals(null, seen)
    }

    @Test
    fun `CoroutineContext accumulates through multiple levels of nested runBlockingV2`() {
        val outerName = CoroutineName("outer")

        data class Seen(val outer: String?, val inner: String?)

        val seen = loomToCoroutines {
            withContext(outerName) {
                yield()
                loomToCoroutines {
                    withContext(CoroutineName("inner")) {
                        yield()
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
        // the innermost withContext wins for the (single-valued) CoroutineName key, but it proves
        // the chain of nested runBlockingV2 calls kept threading the ambient context through.
        assertEquals(Seen(outer = "inner", inner = "inner"), seen)
    }

    @Test
    fun `independent top-level runBlockingV2 calls on different threads never see each other's CoroutineContext`() {
        val callers = 100
        val mismatches = AtomicInteger(0)

        val threads = List(callers) { i ->
            Thread {
                val expected = "caller-$i"
                val seen = loomToCoroutines {
                    withContext(CoroutineName(expected)) {
                        yield() // give other callers a chance to interleave
                        loomToCoroutines { coroutineContext[CoroutineName]?.name }
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
    fun `CoroutineContext propagates through a Java StructuredTaskScope fork into a nested runBlockingV2`() {
        val seen = loomToCoroutines {
            withContext(CoroutineName("across-the-fork")) {
                yield() // force a real redispatch so contextAsScopedValue picks up the new name
                structuredTaskScope {
                    fork { loomToCoroutines { coroutineContext[CoroutineName]?.name } }
                }.get()
            }
        }
        assertEquals("across-the-fork", seen)
    }

    @Test
    fun `a coroutine's own Job is not leaked into a nested runBlockingV2's context`() {
        // each runBlockingV2 launches its own root Job; inheriting the *outer* Job would make the
        // inner one a child of a coroutine that has already returned by the time the inner starts.
        var outerJob: kotlinx.coroutines.Job? = null
        var innerJob: kotlinx.coroutines.Job? = null
        loomToCoroutines {
            outerJob = coroutineContext[kotlinx.coroutines.Job]
            loomToCoroutines {
                innerJob = coroutineContext[kotlinx.coroutines.Job]
            }
        }
        assertTrue(outerJob !== innerJob)
    }
}
