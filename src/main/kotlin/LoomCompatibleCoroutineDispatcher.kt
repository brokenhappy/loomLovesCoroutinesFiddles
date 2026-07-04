import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext

// A Dispatchers.Default-like dispatcher backed by one virtual thread per dispatched task, so each
// task's thread is exclusively its own (no reuse) - which is what makes physically interrupting it
// on cancellation safe. Every dispatch() call: (1) carries over whatever ScopedValues are bound on
// the calling thread, via captureAllScopedValueBindings/overwriteAllValues, and (2) interrupts its
// worker thread the moment the coroutine's Job starts cancelling, so blocking calls that don't
// otherwise participate in cooperative cancellation (Thread.sleep, StructuredTaskScope.join, ...)
// wake up immediately instead of running to completion unnoticed.
internal class LoomCompatibleCoroutineDispatcher(val bindings: ScopedBindings) : ExecutorCoroutineDispatcher() {
    companion object {
        private val executorService = Executors.newVirtualThreadPerTaskExecutor()
        private val delegate = executorService.asCoroutineDispatcher()
    }


    override val executor: Executor get() = executorService

    @OptIn(InternalCoroutinesApi::class)
    override fun dispatch(context: CoroutineContext, block: Runnable) {
        val job = context[Job]
        delegate.dispatch(context) {
            val interruptOnCancel = job?.let(::InterruptOnCancel)
            try {
                bindings.overwriteAllValues { block.run() }
            } finally {
                interruptOnCancel?.clear()
            }
        }
    }

    override fun close() = TODO("Idk why this would happen atm")
}

private const val WORKING = 0
private const val FINISHED = 1
private const val INTERRUPTING = 2
private const val INTERRUPTED = 3

// Same 4-state handshake kotlinx.coroutines' own runInterruptible uses internally (see its
// ThreadState): a cancellation can race with the task's natural completion, so this guarantees we
// never interrupt a thread that has already moved on, and never leave a stray interrupt flag set
// once clear() returns.
@OptIn(InternalCoroutinesApi::class)
private class InterruptOnCancel(job: Job) {
    private val state = AtomicInteger(WORKING)
    private val targetThread = Thread.currentThread()
    private val handle: DisposableHandle = job.invokeOnCompletion(onCancelling = true, invokeImmediately = true) { cause ->
        if (cause != null && state.compareAndSet(WORKING, INTERRUPTING)) {
            targetThread.interrupt()
            state.set(INTERRUPTED)
        }
    }

    fun clear() {
        while (true) {
            when (state.get()) {
                WORKING -> if (state.compareAndSet(WORKING, FINISHED)) {
                    handle.dispose()
                    return
                }
                INTERRUPTING -> Thread.onSpinWait()
                else -> { // INTERRUPTED
                    Thread.interrupted() // drop the flag so it can't leak past this task
                    return
                }
            }
        }
    }
}
