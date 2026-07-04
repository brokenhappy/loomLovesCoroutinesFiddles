import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.StructuredTaskScope

// Thin wrapper around StructuredTaskScope to hide its rough edges: the Callable/Runnable overload
// ambiguity on fork() (Kotlin lambdas match both), the ExecutionException wrapping on join(), and
// the explicit generic witnesses StructuredTaskScope.open() otherwise needs. Fixed to the
// "wait for everyone, cancel the rest and rethrow on first failure" joiner, which covers the common
// case. ScopedValues bound on the calling thread propagate into fork {} automatically - that's
// built into StructuredTaskScope itself, nothing extra needed here.
//
// loomScope {} joins (and unwraps ExecutionException) after the block returns, so fork {} results
// are only safe to .get() once you're back outside the block - e.g. `loomScope { fork { ... } }.get()`.
class LoomScope @PublishedApi internal constructor(private val scope: StructuredTaskScope<Any?, Void, ExecutionException>) {
    fun <T> fork(block: () -> T): StructuredTaskScope.Subtask<T> = scope.fork(Callable(block))
}

inline fun <T> structuredTaskScope(block: LoomScope.() -> T): T {
    StructuredTaskScope.open<Any?, Void, ExecutionException>(
        StructuredTaskScope.Joiner.awaitAllSuccessfulOrThrow(),
    ).use { scope ->
        try {
            val result = LoomScope(scope).block()
            scope.join()
            return result
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }
    }
}
