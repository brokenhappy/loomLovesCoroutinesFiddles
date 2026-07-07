import kotlinx.coroutines.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.LockSupport
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

// The two crossing points between "plain blocking Loom code" and "coroutines". Deliberately not a
// CoroutineDispatcher: dispatching every single hop through a Loom-aware dispatcher had real,
// structural gaps (withContext's "undispatched" fast path skips dispatch() entirely so a
// ScopedValue mirror kept going stale, Delay needed special-casing, cancellation-to-interruption
// needed a hand-rolled state machine). Restricting Loom-awareness to just these two explicit
// crossing points removes all of that: ScopedValues only need capturing/reinstalling right here,
// and kotlinx.coroutines' own runInterruptible already does cancellation-to-interruption correctly,
// so there's no dispatcher, no Delay concern, and no custom interrupt state machine to maintain.

private val virtualThreadDispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()

// Mirrors "the CoroutineContext at the last loomToCoroutines/coroutinesToLoom crossing" as a real
// ScopedValue, since code on the Loom side of the boundary has no other way to read it back: plain
// CoroutineContext propagation only works within coroutine-land.
private val contextAsScopedValue = ScopedValue.newInstance<CoroutineContext>()

// Carries the ScopedValues captured where a Loom thread crossed into coroutines. Plain
// CoroutineContext.Element propagation (not a thread-bound ScopedValue) is what lets this survive
// dispatcher hops and suspensions without any special dispatcher.
private class LoomBindingsElement(val bindings: ScopedBindings) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<LoomBindingsElement>
}

/**
 * Enters the coroutine world, and opens the option to enter back into the Loom world through [coroutinesToLoom].
 *
 * It's important to note that without this, the [ScopedValue]s and interrupting semantics of Loom WILL go lost.
 *
 * So whenever you call a function that now, or might at some point in the future:
 *  - Might block and require interrupting semantics
 *  - Uses [ScopedValue]s under the hood.
 * Then you must call [coroutinesToLoom] to return to the Loom world.
 */
@Throws(InterruptedException::class)
fun <T> loomToCoroutines(block: suspend CoroutineScope.() -> T): T {
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
        else EmptyCoroutineContext

    @OptIn(DelicateCoroutinesApi::class)
    val job = GlobalScope.launch(
        LoomBindingsElement(captureAllScopedValueBindings()).plus(inheritedContext)
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

// Runs block on a fresh virtual thread (via runInterruptible(virtualThreadDispatcher)), with the
// ScopedValues captured at the enclosing loomToCoroutines reinstalled, and this coroutine's own
// CoroutineContext re-exposed as contextAsScopedValue so a nested loomToCoroutines called from
// within block can find it. Cancelling this coroutine while block runs interrupts that thread -
// courtesy of runInterruptible, not anything custom here.
suspend fun <T> coroutinesToLoom(unwrapExecutionException: Boolean = true, block: () -> T): T {
    val context = currentCoroutineContext()
    val bindings = context[LoomBindingsElement]?.bindings
    return runInterruptible(virtualThreadDispatcher) {
        val withContextExposed: () -> T = {
            ScopedValue.where(contextAsScopedValue, context).call<T, Throwable> {
                try {
                    block()
                } catch (e: ExecutionException) {
                    throw if (unwrapExecutionException) (e.cause ?: e) else e
                }
            }
        }
        bindings?.overwriteAllValues(withContextExposed) ?: withContextExposed()
    }
}
