import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import kotlin.coroutines.CoroutineContext
import kotlin.jvm.Throws

private val contextAsScopedValue = ScopedValue.newInstance<CoroutineContext>()

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

// A Dispatchers.Default-like dispatcher backed by one virtual thread per dispatched task, so each
// task's thread is exclusively its own (no reuse) - which is what makes physically interrupting it
// on cancellation safe. Every dispatch() call: (1) carries over whatever ScopedValues are bound on
// the calling thread, via captureAllScopedValueBindings/overwriteAllValues, and (2) interrupts its
// worker thread the moment the coroutine's Job starts cancelling, so blocking calls that don't
// otherwise participate in cooperative cancellation (Thread.sleep, StructuredTaskScope.join, ...)
// wake up immediately instead of running to completion unnoticed.
private class LoomCompatibleCoroutineDispatcher(val bindings: ScopedBindings) : ExecutorCoroutineDispatcher() {
    companion object {
        private val executorService = Executors.newVirtualThreadPerTaskExecutor()
        private val delegate = executorService.asCoroutineDispatcher()
    }


    override val executor: Executor get() = executorService

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        val job = context[Job]
        delegate.dispatch(context) {
            var handle: DisposableHandle? = null
            // Same 4-state handshake kotlinx.coroutines' own runInterruptible uses internally (see its
            // ThreadState): a cancellation can race with the task's natural completion, so this guarantees we
            // never interrupt a thread that has already moved on, and never leave a stray interrupt flag set
            // once clear() returns.
            val state = AtomicInteger(WORKING)
            if (job != null) {
                val targetThread = Thread.currentThread()
                @OptIn(InternalCoroutinesApi::class)
                handle = job.invokeOnCompletion(onCancelling = true, invokeImmediately = true) { cause ->
                    if (cause != null && state.compareAndSet(WORKING, INTERRUPTING)) {
                        targetThread.interrupt()
                        state.set(INTERRUPTED)
                    }
                }
            }
            try {
                bindings.overwriteAllValues {
                    ScopedValue.where(contextAsScopedValue, context).run {
                        block.run()
                    }
                }
            } finally {
                if (job != null) {
                    while (true) {
                        when (state.get()) {
                            WORKING -> if (state.compareAndSet(WORKING, FINISHED)) {
                                handle?.dispose()
                                break
                            }
                            INTERRUPTING -> Thread.onSpinWait()
                            else -> { // INTERRUPTED
                                Thread.interrupted() // drop the flag so it can't leak past this task
                                break
                            }
                        }
                    }
                }
            }
        }
    }

    override fun close() = TODO("Idk why this would happen atm")
}

suspend fun <T> coroutinesToLoom(unwrapExecutionException: Boolean = true, block: () -> T): T {
    return try {
        ScopedValue.where(contextAsScopedValue, currentCoroutineContext()).call<T, Throwable> {
            block()
        }
    } catch (t: Throwable) {
        val job = currentCoroutineContext().job
        val exception = if (unwrapExecutionException) (t as? ExecutionException)?.cause ?: t else t
        @OptIn(InternalCoroutinesApi::class)
        throw if (job.isActive) {
            job.getCancellationException().apply { addSuppressed(exception) }
        } else exception
    }
}

private const val WORKING = 0
private const val FINISHED = 1
private const val INTERRUPTING = 2
private const val INTERRUPTED = 3

