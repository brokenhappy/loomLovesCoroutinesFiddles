import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class LoomCompatibleCoroutineDispatcherTest {
    @Test
    fun `dispatched coroutine sees the ScopedValues bound at the call site`() {
        val requestId = ScopedValue.newInstance<String>()
        val seen = ScopedValue.where(requestId, "abc-123").call<String, RuntimeException> {
            runBlocking(LoomCompatibleCoroutineDispatcher(captureAllScopedValueBindings())) {
                requestId.get()
            }
        }
        assertEquals("abc-123", seen)
    }

    @Test
    fun `cancelling the job interrupts the thread running on it`() {
        val started = CountDownLatch(1)
        val interrupted = CountDownLatch(1)

        val job = CoroutineScope(LoomCompatibleCoroutineDispatcher(captureAllScopedValueBindings())).launch {
            started.countDown()
            try {
                Thread.sleep(60_000)
            } catch (_: InterruptedException) {
                interrupted.countDown()
            }
        }

        assertTrue(started.await(5, TimeUnit.SECONDS))
        job.cancel()
        assertTrue(interrupted.await(5, TimeUnit.SECONDS))
    }

    @Test
    fun `ScopedValues are still bound after a delay() suspension`() {
        val requestId = ScopedValue.newInstance<String>()
        val seen = ScopedValue.where(requestId, "after-delay").call<String, RuntimeException> {
            runBlocking(LoomCompatibleCoroutineDispatcher(captureAllScopedValueBindings())) {
                delay(50.milliseconds)
                requestId.get()
            }
        }
        assertEquals("after-delay", seen)
    }

    @Test
    fun `execution resumes on the dispatcher's own virtual threads after a delay(), not the global default delay thread`() {
        val isVirtual = runBlocking(LoomCompatibleCoroutineDispatcher(captureAllScopedValueBindings())) {
            delay(50.milliseconds)
            Thread.currentThread().isVirtual
        }
        assertTrue(isVirtual)
    }

    @Test
    fun `cancelling a coroutine suspended in delay() completes via normal cancellation, not interruption`() {
        val started = CountDownLatch(1)
        val job = CoroutineScope(LoomCompatibleCoroutineDispatcher(captureAllScopedValueBindings())).launch {
            started.countDown()
            delay(7.seconds)
        }
        assertTrue(started.await(5, TimeUnit.SECONDS))
        job.cancel()
        runBlocking { job.join() }
        assertTrue(job.isCancelled)
    }
}
