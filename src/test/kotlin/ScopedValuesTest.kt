import loom.ScopedBindings
import loom.captureAllScopedValueBindings
import loom.overwriteAllValues
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ScopedValuesTest {
    @Test
    fun `overwriteAllValues restores bindings captured across nested scopes`() {
        val a = ScopedValue.newInstance<Int>()
        val b = ScopedValue.newInstance<String>()
        lateinit var captured: ScopedBindings

        ScopedValue.where(a, 1).run {
            ScopedValue.where(b, "two").run {
                captured = captureAllScopedValueBindings()
            }
        }

        var seenA = -1
        var seenB = ""
        captured.overwriteAllValues {
            seenA = a.get()
            seenB = b.get()
        }
        assertEquals(1, seenA)
        assertEquals("two", seenB)
    }

    @Test
    fun `overwriteAllValues fully replaces the current thread's bindings, it does not layer`() {
        val a = ScopedValue.newInstance<Int>()
        val b = ScopedValue.newInstance<Int>()

        val bBoundOnly = ScopedValue.where(b, 2).call<ScopedBindings, RuntimeException> { captureAllScopedValueBindings() }

        ScopedValue.where(a, 1).run {
            bBoundOnly.overwriteAllValues {
                assertFalse(a.isBound)
                assertEquals(2, b.get())
            }
            // restored after the block
            assertEquals(1, a.get())
            assertFalse(b.isBound)
        }
    }

    @Test
    fun `captureAll works fine when nothing is bound`() {
        val empty = captureAllScopedValueBindings()
        var ran = false
        empty.overwriteAllValues { ran = true }
        assertTrue(ran)
    }
}
