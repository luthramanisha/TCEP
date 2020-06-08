package tcep.utils

/**
  * Created by raheel on 16/01/2018.
  */

import java.lang.management.ManagementFactory
import java.lang.reflect.{Field, Modifier}
import java.util.{Arrays, IdentityHashMap, Random}

import akka.actor.ActorRef
import com.google.common.collect.MapMaker
import com.google.common.hash.Hashing
import org.slf4j.LoggerFactory
import tcep.data.Events.{Event1, Event2, Event3, Event4, Event5, Event6}
import tcep.graph.nodes.traits.Node.{Subscribe, UnSubscribe}
import tcep.graph.transition.{StartExecution, StartExecutionWithData, TransferredState, TransitionRequest}
import tcep.machinenodes.helper.actors.{ACK, CreateRemoteOperator, LoadRequest, LoadResponse, RemoteOperatorCreated, StarksTask, StarksTaskReply}

import scala.collection.mutable.ArrayBuffer
import scala.reflect._
import scala.runtime.ScalaRunTime


/**
  * A trait that allows a class to give [[SizeEstimator]] more accurate size estimation.
  * When a class extends it, [[SizeEstimator]] will query the `estimatedSize` first.
  * If `estimatedSize` does not return [[None]], [[SizeEstimator]] will use the returned size
  * as the size of the object. Otherwise, [[SizeEstimator]] will do the estimation work.
  *
  */
private trait KnownSizeEstimation {
  def estimatedSize: Long
}

/**
  * Estimates the sizes of Java objects (number of bytes of memory they occupy), for use in
  * memory-aware caches.
  *
  * Based on the following JavaWorld article:
  * Based on Spark's SizeEstimator class
  */
object SizeEstimator {

  /**
    * Estimate the number of bytes that the given object takes up on the JVM heap. The estimate
    * includes space taken up by objects referenced by the given object, their references, and so on
    * and so forth.
    *
    * This is useful for determining the amount of heap space a broadcast variable will occupy on
    * each executor or the amount of space each object will take when caching objects in
    * deserialized form. This is not the same as the serialized size of the object, which will
    * typically be much smaller.
    */
  def estimate(obj: AnyRef): Long = {
    // need to treat messages separately since they can not be properly handled by SizeEstimator implementation
    // (e.g. estimate of 3.5MB, but actually only a few hundred bytes)
    // values obtained through logging messages size via akka.remote.classic.log-frame-size-exceeding = ... in application.conf
    obj match {
      case Subscribe(_) => 438
      case UnSubscribe() => 255
      case CreateRemoteOperator(_, _) => 2000
      case RemoteOperatorCreated(_) => 426
      case TransitionRequest(_, _, _) => 542
      case TransferredState(_, _, _, _) => 650
      case StartExecution(_) => 268
      case StartExecutionWithData(_, _, _, data, _) => 512 + data.size * 480
      case LoadRequest() => 167
      case LoadResponse(_) => 250
      case StarksTask(_, _, _) => 1243
      case StarksTaskReply(_) => 1400
      case ACK() => 250
      case e: Event1 => 480
      case e: Event2 => 488
      case e: Event3 => 496
      case e: Event4 => 504
      case e: Event5 => 512
      case e: Event6 => 520
      case _ =>
        log.debug(s"estimating size of object $obj, result may not be reliable")
        estimate(obj, new IdentityHashMap[AnyRef, AnyRef])
    }
  }

  // Sizes of primitive types
  private val BYTE_SIZE = 1
  private val BOOLEAN_SIZE = 1
  private val CHAR_SIZE = 2
  private val SHORT_SIZE = 2
  private val INT_SIZE = 4
  private val LONG_SIZE = 8
  private val FLOAT_SIZE = 4
  private val DOUBLE_SIZE = 8

  // Fields can be primitive types, sizes are: 1, 2, 4, 8. Or fields can be pointers. The size of
  // a pointer is 4 or 8 depending on the JVM (32-bit or 64-bit) and UseCompressedOops flag.
  // The sizes should be in descending order, as we will use that information for fields placement.
  private val fieldSizes = List(8, 4, 2, 1)

  // Alignment boundary for objects
  // TODO: Is this arch dependent ?
  private val ALIGN_SIZE = 8

  // A cache of ClassInfo objects for each class
  // We use weakKeys to allow GC of dynamically created classes
  private val classInfos = new MapMaker().weakKeys().makeMap[Class[_], ClassInfo]()

  // Object and pointer sizes are arch dependent
  private var is64bit = false

  // Size of an object reference
  // Based on https://wikis.oracle.com/display/HotSpotInternals/CompressedOops
  private var isCompressedOops = false
  private var pointerSize = 4

  // Minimum size of a java.lang.Object
  private var objectSize = 8

  val log = LoggerFactory.getLogger(getClass)

  initialize()

  // Sets object size, pointer size based on architecture and CompressedOops settings
  // from the JVM.
  private def initialize() {
    val arch = System.getProperty("os.arch")
    is64bit = arch.contains("64") || arch.contains("s390x")
    isCompressedOops = getIsCompressedOops

    objectSize = if (!is64bit) 8 else {
      if (!isCompressedOops) {
        16
      } else {
        12
      }
    }
    pointerSize = if (is64bit && !isCompressedOops) 8 else 4
    classInfos.clear()
    classInfos.put(classOf[Object], new ClassInfo(objectSize, Nil))
  }

  private def getIsCompressedOops: Boolean = {
    // This is only used by tests to override the detection of compressed oops. The test
    // actually uses a system property instead of a SparkConf, so we'll stick with that.
    if (System.getProperty("spark.test.useCompressedOops") != null) {
      return System.getProperty("spark.test.useCompressedOops").toBoolean
    }

    // java.vm.info provides compressed ref info for IBM JDKs
    if (System.getProperty("java.vendor").contains("IBM")) {
      return System.getProperty("java.vm.info").contains("Compressed Ref")
    }

    try {
      val hotSpotMBeanName = "com.sun.management:type=HotSpotDiagnostic"
      val server = ManagementFactory.getPlatformMBeanServer()

      // NOTE: This should throw an exception in non-Sun JVMs
      // scalastyle:off classforname
      val hotSpotMBeanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean")
      val getVMMethod = hotSpotMBeanClass.getDeclaredMethod("getVMOption",
        Class.forName("java.lang.String"))
      // scalastyle:on classforname

      val bean = ManagementFactory.newPlatformMXBeanProxy(server,
        hotSpotMBeanName, hotSpotMBeanClass)
      // TODO: We could use reflection on the VMOption returned ?
      getVMMethod.invoke(bean, "UseCompressedOops").toString.contains("true")
    } catch {
      case e: Exception =>
        // Guess whether they've enabled UseCompressedOops based on whether maxMemory < 32 GB
        val guess = Runtime.getRuntime.maxMemory < (32L*1024*1024*1024)
        val guessInWords = if (guess) "yes" else "not"
        log.warn("Failed to check whether UseCompressedOops is set; assuming " + guessInWords)
        return guess
    }
  }

  /**
    * The state of an ongoing size estimation. Contains a stack of objects to visit as well as an
    * IdentityHashMap of visited objects, and provides utility methods for enqueueing new objects
    * to visit.
    */
  private class SearchState(val visited: IdentityHashMap[AnyRef, AnyRef]) {
    val stack = new ArrayBuffer[AnyRef]
    var size = 0L

    def enqueue(obj: AnyRef) {
      if (obj != null && !visited.containsKey(obj)) {
        visited.put(obj, null)
        stack += obj
      }
    }

    def isFinished(): Boolean = stack.isEmpty

    def dequeue(): AnyRef = {
      val elem = stack.last
      stack.trimEnd(1)
      elem
    }
  }

  /**
    * Cached information about each class. We remember two things: the "shell size" of the class
    * (size of all non-static fields plus the java.lang.Object size), and any fields that are
    * pointers to objects.
    */
  private class ClassInfo(
                           val shellSize: Long,
                           val pointerFields: List[Field]) {}

  private def estimate(obj: AnyRef, visited: IdentityHashMap[AnyRef, AnyRef]): Long = {
    val state = new SearchState(visited)
    state.enqueue(obj)
    while (!state.isFinished) {
      visitSingleObject(state.dequeue(), state)
    }
    state.size
  }

  private def visitSingleObject(obj: AnyRef, state: SearchState) {
    val cls = obj.getClass
    if (cls.isArray) {
      visitArray(obj, cls, state)
    } else if (cls.getName.startsWith("scala.reflect")) {
      // Many objects in the scala.reflect package reference global reflection objects which, in
      // turn, reference many other large global objects. Do nothing in this case.
    } else if (obj.isInstanceOf[ClassLoader] || obj.isInstanceOf[Class[_]]) {
      // Hadoop JobConfs created in the interpreter have a ClassLoader, which greatly confuses
      // the size estimator since it references the whole REPL. Do nothing in this case. In
      // general all ClassLoaders and Classes will be shared between objects anyway.
    } else {
      obj match {
        case s: KnownSizeEstimation =>
          state.size += s.estimatedSize
        case _ =>
          val classInfo = getClassInfo(cls)
          state.size += alignSize(classInfo.shellSize)
          for (field <- classInfo.pointerFields) {
            state.enqueue(field.get(obj))
          }
      }
    }
  }

  // Estimate the size of arrays larger than ARRAY_SIZE_FOR_SAMPLING by sampling.
  private val ARRAY_SIZE_FOR_SAMPLING = 400
  private val ARRAY_SAMPLE_SIZE = 100 // should be lower than ARRAY_SIZE_FOR_SAMPLING

  private def visitArray(array: AnyRef, arrayClass: Class[_], state: SearchState) {
    val length = ScalaRunTime.array_length(array)
    val elementClass = arrayClass.getComponentType()

    // Arrays have object header and length field which is an integer
    var arrSize: Long = alignSize(objectSize + INT_SIZE)

    if (elementClass.isPrimitive) {
      arrSize += alignSize(length.toLong * primitiveSize(elementClass))
      state.size += arrSize
    } else {
      arrSize += alignSize(length.toLong * pointerSize)
      state.size += arrSize

      if (length <= ARRAY_SIZE_FOR_SAMPLING) {
        var arrayIndex = 0
        while (arrayIndex < length) {
          state.enqueue(ScalaRunTime.array_apply(array, arrayIndex).asInstanceOf[AnyRef])
          arrayIndex += 1
        }
      } else {
        // Estimate the size of a large array by sampling elements without replacement.
        // To exclude the shared objects that the array elements may link, sample twice
        // and use the min one to calculate array size.
        val rand = new Random(42)
        val drawn = new OpenHashSet[Int](2 * ARRAY_SAMPLE_SIZE)
        val s1 = sampleArray(array, state, rand, drawn, length)
        val s2 = sampleArray(array, state, rand, drawn, length)
        val size = math.min(s1, s2)
        state.size += math.max(s1, s2) +
          (size * ((length - ARRAY_SAMPLE_SIZE) / (ARRAY_SAMPLE_SIZE))).toLong
      }
    }
  }

  private def sampleArray(
                           array: AnyRef,
                           state: SearchState,
                           rand: Random,
                           drawn: OpenHashSet[Int],
                           length: Int): Long = {
    var size = 0L
    for (i <- 0 until ARRAY_SAMPLE_SIZE) {
      var index = 0
      do {
        index = rand.nextInt(length)
      } while (drawn.contains(index))
      drawn.add(index)
      val obj = ScalaRunTime.array_apply(array, index).asInstanceOf[AnyRef]
      if (obj != null) {
        size += SizeEstimator.estimate(obj, state.visited).toLong
      }
    }
    size
  }

  private def primitiveSize(cls: Class[_]): Int = {
    if (cls == classOf[Byte]) {
      BYTE_SIZE
    } else if (cls == classOf[Boolean]) {
      BOOLEAN_SIZE
    } else if (cls == classOf[Char]) {
      CHAR_SIZE
    } else if (cls == classOf[Short]) {
      SHORT_SIZE
    } else if (cls == classOf[Int]) {
      INT_SIZE
    } else if (cls == classOf[Long]) {
      LONG_SIZE
    } else if (cls == classOf[Float]) {
      FLOAT_SIZE
    } else if (cls == classOf[Double]) {
      DOUBLE_SIZE
    } else {
      throw new IllegalArgumentException(
        "Non-primitive class " + cls + " passed to primitiveSize()")
    }
  }

  /**
    * Get or compute the ClassInfo for a given class.
    */
  private def getClassInfo(cls: Class[_]): ClassInfo = {
    // Check whether we've already cached a ClassInfo for this class
    val info = classInfos.get(cls)
    if (info != null) {
      return info
    }

    val parent = getClassInfo(cls.getSuperclass)
    var shellSize = parent.shellSize
    var pointerFields = parent.pointerFields
    val sizeCount = Array.fill(fieldSizes.max + 1)(0)

    // iterate through the fields of this class and gather information.
    for (field <- cls.getDeclaredFields) {
      if (!Modifier.isStatic(field.getModifiers)) {
        val fieldClass = field.getType
        if (fieldClass.isPrimitive) {
          sizeCount(primitiveSize(fieldClass)) += 1
        } else {
          field.setAccessible(true) // Enable future get()'s on this field
          sizeCount(pointerSize) += 1
          pointerFields = field :: pointerFields
        }
      }
    }

    // Based on the simulated field layout code in Aleksey Shipilev's report:
    // http://cr.openjdk.java.net/~shade/papers/2013-shipilev-fieldlayout-latest.pdf
    // The code is in Figure 9.
    // The simplified idea of field layout consists of 4 parts (see more details in the report):
    //
    // 1. field alignment: HotSpot lays out the fields aligned by their size.
    // 2. object alignment: HotSpot rounds instance size up to 8 bytes
    // 3. consistent fields layouts throughout the hierarchy: This means we should layout
    // superclass first. And we can use superclass's shellSize as a starting point to layout the
    // other fields in this class.
    // 4. class alignment: HotSpot rounds field blocks up to HeapOopSize not 4 bytes, confirmed
    // with Aleksey. see https://bugs.openjdk.java.net/browse/CODETOOLS-7901322
    //
    // The real world field layout is much more complicated. There are three kinds of fields
    // order in Java 8. And we don't consider the @contended annotation introduced by Java 8.
    // see the HotSpot classloader code, layout_fields method for more details.
    // hg.openjdk.java.net/jdk8/jdk8/hotspot/file/tip/src/share/vm/classfile/classFileParser.cpp
    var alignedSize = shellSize
    for (size <- fieldSizes if sizeCount(size) > 0) {
      val count = sizeCount(size).toLong
      // If there are internal gaps, smaller field can fit in.
      alignedSize = math.max(alignedSize, alignSizeUp(shellSize, size) + size * count)
      shellSize += size * count
    }

    // Should choose a larger size to be new shellSize and clearly alignedSize >= shellSize, and
    // round up the instance filed blocks
    shellSize = alignSizeUp(alignedSize, pointerSize)

    // Create and cache a new ClassInfo
    val newInfo = new ClassInfo(shellSize, pointerFields)
    classInfos.put(cls, newInfo)
    newInfo
  }

  private def alignSize(size: Long): Long = alignSizeUp(size, ALIGN_SIZE)

  /**
    * Compute aligned size. The alignSize must be 2^n, otherwise the result will be wrong.
    * When alignSize = 2^n, alignSize - 1 = 2 ^^ (n - 1). The binary representation of (alignSize - 1)
    * will only have n trailing 1s(0b00...001..1). ~(alignSize - 1) will be 0b11..110..0. Hence,
    * (size + alignSize - 1) & ~(alignSize - 1) will set the last n bits to zeros, which leads to
    * multiple of alignSize.
    */
  private def alignSizeUp(size: Long, alignSize: Int): Long =
    (size + alignSize - 1) & ~(alignSize - 1)
}

class OpenHashSet[@specialized(Long, Int) T: ClassTag](
                                                        initialCapacity: Int,
                                                        loadFactor: Double)
  extends Serializable {

  require(initialCapacity <= OpenHashSet.MAX_CAPACITY,
    s"Can't make capacity bigger than ${OpenHashSet.MAX_CAPACITY} elements")
  require(initialCapacity >= 0, "Invalid initial capacity")
  require(loadFactor < 1.0, "Load factor must be less than 1.0")
  require(loadFactor > 0.0, "Load factor must be greater than 0.0")

  import OpenHashSet._

  def this(initialCapacity: Int) = this(initialCapacity, 0.7)

  def this() = this(64)

  // The following member variables are declared as protected instead of private for the
  // specialization to work (specialized class extends the non-specialized one and needs access
  // to the "private" variables).

  val hasher: Hasher[T] = {
    // It would've been more natural to write the following using pattern matching. But Scala 2.9.x
    // compiler has a bug when specialization is used together with this pattern matching, and
    // throws:
    // scala.tools.nsc.symtab.Types$TypeError: type mismatch;
    //  found   : scala.reflect.AnyValManifest[Long]
    //  required: scala.reflect.ClassTag[Int]
    //         at scala.tools.nsc.typechecker.Contexts$Context.error(Contexts.scala:298)
    //         at scala.tools.nsc.typechecker.Infer$Inferencer.error(Infer.scala:207)
    //         ...
    val mt = classTag[T]
    if (mt == ClassTag.Long) {
      (new LongHasher).asInstanceOf[Hasher[T]]
    } else if (mt == ClassTag.Int) {
      (new IntHasher).asInstanceOf[Hasher[T]]
    } else {
      new Hasher[T]
    }
  }

  protected var _capacity = nextPowerOf2(initialCapacity)
  protected var _mask = _capacity - 1
  protected var _size = 0
  protected var _growThreshold = (loadFactor * _capacity).toInt

  protected var _bitset = new BitSet(_capacity)

  def getBitSet: BitSet = _bitset

  // Init of the array in constructor (instead of in declaration) to work around a Scala compiler
  // specialization bug that would generate two arrays (one for Object and one for specialized T).
  protected var _data: Array[T] = _
  _data = new Array[T](_capacity)

  /** Number of elements in the set. */
  def size: Int = _size

  /** The capacity of the set (i.e. size of the underlying array). */
  def capacity: Int = _capacity

  /** Return true if this set contains the specified element. */
  def contains(k: T): Boolean = getPos(k) != INVALID_POS

  /**
    * Add an element to the set. If the set is over capacity after the insertion, grow the set
    * and rehash all elements.
    */
  def add(k: T) {
    addWithoutResize(k)
    rehashIfNeeded(k, grow, move)
  }

  def union(other: OpenHashSet[T]): OpenHashSet[T] = {
    val iterator = other.iterator
    while (iterator.hasNext) {
      add(iterator.next())
    }
    this
  }

  /**
    * Add an element to the set. This one differs from add in that it doesn't trigger rehashing.
    * The caller is responsible for calling rehashIfNeeded.
    *
    * Use (retval & POSITION_MASK) to get the actual position, and
    * (retval & NONEXISTENCE_MASK) == 0 for prior existence.
    *
    * @return The position where the key is placed, plus the highest order bit is set if the key
    *         does not exists previously.
    */
  def addWithoutResize(k: T): Int = {
    var pos = hashcode(hasher.hash(k)) & _mask
    var delta = 1
    while (true) {
      if (!_bitset.get(pos)) {
        // This is a new key.
        _data(pos) = k
        _bitset.set(pos)
        _size += 1
        return pos | NONEXISTENCE_MASK
      } else if (_data(pos) == k) {
        // Found an existing key.
        return pos
      } else {
        // quadratic probing with values increase by 1, 2, 3, ...
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.")
  }

  /**
    * Rehash the set if it is overloaded.
    * @param k A parameter unused in the function, but to force the Scala compiler to specialize
    *          this method.
    * @param allocateFunc Callback invoked when we are allocating a new, larger array.
    * @param moveFunc Callback invoked when we move the key from one position (in the old data array)
    *                 to a new position (in the new data array).
    */
  def rehashIfNeeded(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit) {
    if (_size > _growThreshold) {
      rehash(k, allocateFunc, moveFunc)
    }
  }

  /**
    * Return the position of the element in the underlying array, or INVALID_POS if it is not found.
    */
  def getPos(k: T): Int = {
    var pos = hashcode(hasher.hash(k)) & _mask
    var delta = 1
    while (true) {
      if (!_bitset.get(pos)) {
        return INVALID_POS
      } else if (k == _data(pos)) {
        return pos
      } else {
        // quadratic probing with values increase by 1, 2, 3, ...
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.")
  }

  /** Return the value at the specified position. */
  def getValue(pos: Int): T = _data(pos)

  def iterator: Iterator[T] = new Iterator[T] {
    var pos = nextPos(0)
    override def hasNext: Boolean = pos != INVALID_POS
    override def next(): T = {
      val tmp = getValue(pos)
      pos = nextPos(pos + 1)
      tmp
    }
  }

  /** Return the value at the specified position. */
  def getValueSafe(pos: Int): T = {
    assert(_bitset.get(pos))
    _data(pos)
  }

  /**
    * Return the next position with an element stored, starting from the given position inclusively.
    */
  def nextPos(fromPos: Int): Int = _bitset.nextSetBit(fromPos)

  /**
    * Double the table's size and re-hash everything. We are not really using k, but it is declared
    * so Scala compiler can specialize this method (which leads to calling the specialized version
    * of putInto).
    *
    * @param k A parameter unused in the function, but to force the Scala compiler to specialize
    *          this method.
    * @param allocateFunc Callback invoked when we are allocating a new, larger array.
    * @param moveFunc Callback invoked when we move the key from one position (in the old data array)
    *                 to a new position (in the new data array).
    */
  private def rehash(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit) {
    val newCapacity = _capacity * 2
    require(newCapacity > 0 && newCapacity <= OpenHashSet.MAX_CAPACITY,
      s"Can't contain more than ${(loadFactor * OpenHashSet.MAX_CAPACITY).toInt} elements")
    allocateFunc(newCapacity)
    val newBitset = new BitSet(newCapacity)
    val newData = new Array[T](newCapacity)
    val newMask = newCapacity - 1

    var oldPos = 0
    while (oldPos < capacity) {
      if (_bitset.get(oldPos)) {
        val key = _data(oldPos)
        var newPos = hashcode(hasher.hash(key)) & newMask
        var i = 1
        var keepGoing = true
        // No need to check for equality here when we insert so this has one less if branch than
        // the similar code path in addWithoutResize.
        while (keepGoing) {
          if (!newBitset.get(newPos)) {
            // Inserting the key at newPos
            newData(newPos) = key
            newBitset.set(newPos)
            moveFunc(oldPos, newPos)
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }

    _bitset = newBitset
    _data = newData
    _capacity = newCapacity
    _mask = newMask
    _growThreshold = (loadFactor * newCapacity).toInt
  }

  /**
    * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
    */
  private def hashcode(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  private def nextPowerOf2(n: Int): Int = {
    if (n == 0) {
      1
    } else {
      val highBit = Integer.highestOneBit(n)
      if (highBit == n) n else highBit << 1
    }
  }
}


object OpenHashSet {

  val MAX_CAPACITY = 1 << 30
  val INVALID_POS = -1
  val NONEXISTENCE_MASK = 1 << 31
  val POSITION_MASK = (1 << 31) - 1

  /**
    * A set of specialized hash function implementation to avoid boxing hash code computation
    * in the specialized implementation of OpenHashSet.
    */
  sealed class Hasher[@specialized(Long, Int) T] extends Serializable {
    def hash(o: T): Int = o.hashCode()
  }

  class LongHasher extends Hasher[Long] {
    override def hash(o: Long): Int = (o ^ (o >>> 32)).toInt
  }

  class IntHasher extends Hasher[Int] {
    override def hash(o: Int): Int = o
  }

  private def grow1(newSize: Int) {}
  private def move1(oldPos: Int, newPos: Int) { }

  private val grow = grow1 _
  private val move = move1 _
}


/**
  * A simple, fixed-size bit set implementation. This implementation is fast because it avoids
  * safety/bound checking.
  */
class BitSet(numBits: Int) extends Serializable {

  private val words = new Array[Long](bit2words(numBits))
  private val numWords = words.length

  /**
    * Compute the capacity (number of bits) that can be represented
    * by this bitset.
    */
  def capacity: Int = numWords * 64

  /**
    * Clear all set bits.
    */
  def clear(): Unit = Arrays.fill(words, 0)

  /**
    * Set all the bits up to a given index
    */
  def setUntil(bitIndex: Int): Unit = {
    val wordIndex = bitIndex >> 6 // divide by 64
    Arrays.fill(words, 0, wordIndex, -1)
    if(wordIndex < words.length) {
      // Set the remaining bits (note that the mask could still be zero)
      val mask = ~(-1L << (bitIndex & 0x3f))
      words(wordIndex) |= mask
    }
  }

  /**
    * Clear all the bits up to a given index
    */
  def clearUntil(bitIndex: Int): Unit = {
    val wordIndex = bitIndex >> 6 // divide by 64
    Arrays.fill(words, 0, wordIndex, 0)
    if(wordIndex < words.length) {
      // Clear the remaining bits
      val mask = -1L << (bitIndex & 0x3f)
      words(wordIndex) &= mask
    }
  }

  /**
    * Compute the bit-wise AND of the two sets returning the
    * result.
    */
  def &(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    val smaller = math.min(numWords, other.numWords)
    assert(newBS.numWords >= numWords)
    assert(newBS.numWords >= other.numWords)
    var ind = 0
    while( ind < smaller ) {
      newBS.words(ind) = words(ind) & other.words(ind)
      ind += 1
    }
    newBS
  }

  /**
    * Compute the bit-wise OR of the two sets returning the
    * result.
    */
  def |(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    assert(newBS.numWords >= numWords)
    assert(newBS.numWords >= other.numWords)
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while( ind < smaller ) {
      newBS.words(ind) = words(ind) | other.words(ind)
      ind += 1
    }
    while( ind < numWords ) {
      newBS.words(ind) = words(ind)
      ind += 1
    }
    while( ind < other.numWords ) {
      newBS.words(ind) = other.words(ind)
      ind += 1
    }
    newBS
  }

  /**
    * Compute the symmetric difference by performing bit-wise XOR of the two sets returning the
    * result.
    */
  def ^(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) ^ other.words(ind)
      ind += 1
    }
    if (ind < numWords) {
      Array.copy( words, ind, newBS.words, ind, numWords - ind )
    }
    if (ind < other.numWords) {
      Array.copy( other.words, ind, newBS.words, ind, other.numWords - ind )
    }
    newBS
  }

  /**
    * Compute the difference of the two sets by performing bit-wise AND-NOT returning the
    * result.
    */
  def andNot(other: BitSet): BitSet = {
    val newBS = new BitSet(capacity)
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) & ~other.words(ind)
      ind += 1
    }
    if (ind < numWords) {
      Array.copy( words, ind, newBS.words, ind, numWords - ind )
    }
    newBS
  }

  /**
    * Sets the bit at the specified index to true.
    * @param index the bit index
    */
  def set(index: Int) {
    val bitmask = 1L << (index & 0x3f)  // mod 64 and shift
    words(index >> 6) |= bitmask        // div by 64 and mask
  }

  def unset(index: Int) {
    val bitmask = 1L << (index & 0x3f)  // mod 64 and shift
    words(index >> 6) &= ~bitmask        // div by 64 and mask
  }

  /**
    * Return the value of the bit with the specified index. The value is true if the bit with
    * the index is currently set in this BitSet; otherwise, the result is false.
    *
    * @param index the bit index
    * @return the value of the bit with the specified index
    */
  def get(index: Int): Boolean = {
    val bitmask = 1L << (index & 0x3f)   // mod 64 and shift
    (words(index >> 6) & bitmask) != 0  // div by 64 and mask
  }

  /**
    * Get an iterator over the set bits.
    */
  def iterator: Iterator[Int] = new Iterator[Int] {
    var ind = nextSetBit(0)
    override def hasNext: Boolean = ind >= 0
    override def next(): Int = {
      val tmp = ind
      ind = nextSetBit(ind + 1)
      tmp
    }
  }


  /** Return the number of bits set to true in this BitSet. */
  def cardinality(): Int = {
    var sum = 0
    var i = 0
    while (i < numWords) {
      sum += java.lang.Long.bitCount(words(i))
      i += 1
    }
    sum
  }

  /**
    * Returns the index of the first bit that is set to true that occurs on or after the
    * specified starting index. If no such bit exists then -1 is returned.
    *
    * To iterate over the true bits in a BitSet, use the following loop:
    *
    *  for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
    *    // operate on index i here
    *  }
    *
    * @param fromIndex the index to start checking from (inclusive)
    * @return the index of the next set bit, or -1 if there is no such bit
    */
  def nextSetBit(fromIndex: Int): Int = {
    var wordIndex = fromIndex >> 6
    if (wordIndex >= numWords) {
      return -1
    }

    // Try to find the next set bit in the current word
    val subIndex = fromIndex & 0x3f
    var word = words(wordIndex) >> subIndex
    if (word != 0) {
      return (wordIndex << 6) + subIndex + java.lang.Long.numberOfTrailingZeros(word)
    }

    // Find the next set bit in the rest of the words
    wordIndex += 1
    while (wordIndex < numWords) {
      word = words(wordIndex)
      if (word != 0) {
        return (wordIndex << 6) + java.lang.Long.numberOfTrailingZeros(word)
      }
      wordIndex += 1
    }

    -1
  }

  /** Return the number of longs it would take to hold numBits. */
  private def bit2words(numBits: Int) = ((numBits - 1) >> 6) + 1
}