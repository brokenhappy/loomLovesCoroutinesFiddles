import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.LockSupport
import kotlin.coroutines.CoroutineContext
import kotlin.jvm.Throws

private val contextAsScopedValue = ScopedValue.newInstance<CoroutineContext>()

@Throws(InterruptedException::class)
fun <T> runBlockingV2(block: suspend CoroutineScope.() -> T): T {
    // Unique type that we can type cast on. Now we only allocate this in the exception case,
    // but the happy path needs no extra allocations.
    class ThrowableWrapper(val throwable: Throwable)

    val thread = Thread.currentThread()
    if (thread.isInterrupted) throw InterruptedException()

    val wasUnparked = AtomicBoolean(false)
    val result = object {
        @Volatile
        var state: Any? = null // Bc Kotlin doesn't support local `@Volatile`s
    }

    val inheritedContext =
        if (contextAsScopedValue.isBound) contextAsScopedValue.get().minusKey(Job)
        else kotlin.coroutines.EmptyCoroutineContext

    @OptIn(DelicateCoroutinesApi::class)
    val job = GlobalScope.launch(
        LoomCompatibleCoroutineDispatcher(captureAllScopedValueBindings()).plus(inheritedContext)
    ) {
        result.state = try {
            block()
        } catch (t: Throwable) {
            ThrowableWrapper(t)
        }
    }.apply {
        invokeOnCompletion {
            wasUnparked.set(true)
            LockSupport.unpark(thread)
        }
    }

    do {
        LockSupport.park()
    } while (!thread.isInterrupted && !wasUnparked.get())


    if (Thread.interrupted()) {
        job.cancel()
        while (!wasUnparked.get()) {
            LockSupport.park()
            Thread.interrupted() // clear flag so we don't busy loop
        }
        thread.interrupt() // Set flag back
    }
    @Suppress("UNCHECKED_CAST")
    val throwable = (result.state as? ThrowableWrapper)?.throwable
    if (thread.isInterrupted) throw InterruptedException().apply {
        throwable?.let { addSuppressed(throwable) }
    }
    throwable?.let { throw it }

    @Suppress("UNCHECKED_CAST")
    return result.state as T
}
