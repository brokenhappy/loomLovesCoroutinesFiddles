package loom

import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType

// Every bound ScopedValue on a thread is already a single O(1)-readable reference: the private
// Thread.scopedValueBindings field (a full linked snapshot of ALL bindings, not just one key).
// ScopedValue.Carrier.run/call swap that single reference in and out around the block they run
// (see Carrier.runWith in java.lang.ScopedValue) and invalidate the small per-thread lookup cache
// that .get() consults. This mirrors that: capture is a field read, install is a field write plus
// a cache invalidation - no walking of Carrier/Snapshot chains needed.
internal class ScopedBindings(val raw: Any?)

internal fun captureAllScopedValueBindings(): ScopedBindings = ScopedBindings(ScopedValueInternals.currentBindings())

internal fun ScopedBindings.overwriteAllValues(block: () -> Unit) {
    val previous = ScopedValueInternals.currentBindings()
    ScopedValueInternals.installBindings(raw)
    try {
        block()
    } finally {
        ScopedValueInternals.installBindings(previous)
    }
}

private object ScopedValueInternals {
    private val snapshotClass: Class<*> = Class.forName("java.lang.ScopedValue\$Snapshot")
    private val cacheClass: Class<*> = Class.forName("java.lang.ScopedValue\$Cache")

    private val threadLookup = MethodHandles.privateLookupIn(Thread::class.java, MethodHandles.lookup())
    private val scopedValueLookup = MethodHandles.privateLookupIn(ScopedValue::class.java, MethodHandles.lookup())

    // Resolves Thread.NEW_THREAD_BINDINGS lazily on first use, same as Carrier.run/call does.
    private val getBindings = scopedValueLookup.findStatic(
        ScopedValue::class.java, "scopedValueBindings", MethodType.methodType(snapshotClass),
    )
    private val setBindings = threadLookup.findStatic(
        Thread::class.java, "setScopedValueBindings", MethodType.methodType(Void.TYPE, Any::class.java),
    )
    // Cache.invalidate is a nestmate of ScopedValue, so ScopedValue's private Lookup can reach it too.
    private val invalidateCache = scopedValueLookup.findStatic(
        cacheClass, "invalidate", MethodType.methodType(Void.TYPE, Int::class.javaPrimitiveType),
    )

    fun currentBindings(): Any? = getBindings.invoke()

    fun installBindings(bindings: Any?) {
        setBindings.invoke(bindings)
        invalidateCache.invoke(-1) // -1: we don't know which keys changed, so drop every cached slot
    }
}
