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

    @OptIn(DelicateCoroutinesApi::class)
    GlobalScope.launch(
        LoomCompatibleCoroutineDispatcher(captureAllScopedValueBindings())
            .plus(contextAsScopedValue.get())
            .minusKey(Job)
    ) {
        result.state = try {
            block()
        } catch (t: Throwable) {
            ThrowableWrapper(t)
        }
    }.invokeOnCompletion {
        wasUnparked.set(true)
        LockSupport.unpark(thread)
    }

    do {
        LockSupport.park()
    } while (!thread.isInterrupted && !wasUnparked.get())

    @Suppress("UNCHECKED_CAST")
    val throwable = (result.state as? ThrowableWrapper)?.throwable
    if (thread.isInterrupted) {
        throw InterruptedException().apply {
            throwable?.let { addSuppressed(throwable) }
        }
    }
    throwable?.let { throw it }

    @Suppress("UNCHECKED_CAST")
    return (result.state as? ThrowableWrapper)
        ?.let { throw it.throwable }
        ?: result as T
}
