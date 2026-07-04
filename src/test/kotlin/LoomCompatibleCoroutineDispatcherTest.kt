import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LoomCompatibleCoroutineDispatcherTest {
    @Test
    fun `dispatched coroutine sees the ScopedValues bound at the call site`() {
        val requestId = ScopedValue.newInstance<String>()
        val dispatcher = LoomCompatibleCoroutineDispatcher()
        try {
            val seen = ScopedValue.where(requestId, "abc-123").call<String, RuntimeException> {
                runBlocking(dispatcher) { requestId.get() }
            }
            assertEquals("abc-123", seen)
        } finally {
            dispatcher.close()
        }
    }

    @Test
    fun `cancelling the job interrupts the thread running on it`() {
        val dispatcher = LoomCompatibleCoroutineDispatcher()
        try {
            val started = CountDownLatch(1)
            val interrupted = CountDownLatch(1)

            val job = CoroutineScope(dispatcher).launch {
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
        } finally {
            dispatcher.close()
        }
    }
}
