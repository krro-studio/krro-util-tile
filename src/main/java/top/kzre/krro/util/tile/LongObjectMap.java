package top.kzre.krro.util.tile;

import java.util.Objects;
import java.util.concurrent.locks.StampedLock;

/**
 * 线程安全、零 Long 装箱的开放寻址哈希表（long → T）。
 *
 * <p>并发语义：
 * <ul>
 *   <li><b>读操作无锁</b>：使用 {@link StampedLock} 乐观读，多读者并发无竞争；
 *       若乐观读期间发生写，则自动降级为读锁重试，保证读到某个一致快照。</li>
 *   <li><b>写操作互斥</b>：{@code put/putIfAbsent/clear} 持有写锁，扩容也在写锁内完成。</li>
 *   <li>{@link #size()} 为 volatile 读，最终一致。</li>
 *   <li>{@link #forEach} 在读锁内遍历，保证遍历期间数组不被修改；
 *       <b>禁止在 consumer 内调用本 map 的写方法</b>（StampedLock 不可重入，会死锁）。</li>
 * </ul>
 *
 * <p>设计取舍：
 * <ul>
 *   <li>线性探测（linear probing），连续内存、缓存友好；</li>
 *   <li>不支持删除（无 remove），因此无需墓碑，探测链始终干净；</li>
 *   <li>以 {@code values[i] == null} 作为空槽标记，因此 <b>不允许 null value</b>，
 *       省去额外的 used 位图；</li>
 *   <li>key 经 {@link #spread(long)} 混合高低位，连续整数 key 也能均匀散列；</li>
 *   <li>负载因子 0.6，偏向查询性能；</li>
 *   <li>容量上限 {@code 1 << 30}，超过后不再扩容。</li>
 * </ul>
 *
 * @param <T> value 类型，不允许为 null
 */
@SuppressWarnings("unchecked")
final class LongObjectMap<T> {

    private static final int MIN_CAPACITY = 16;
    private static final int MAX_CAPACITY = 1 << 30;
    private static final float LOAD_FACTOR = 0.6f;

    /** 不可变表快照：扩容时整体替换，读者读到的永远是一份自洽的数组。 */
    private static final class Table<T> {
        final long[] keys;
        final Object[] values;
        final int mask;
        final int threshold;

        Table(int capacity) {
            this.keys = new long[capacity];
            this.values = new Object[capacity];
            this.mask = capacity - 1;
            this.threshold = Math.max(1, (int) (capacity * LOAD_FACTOR));
        }
    }

    private final StampedLock lock = new StampedLock();
    private volatile Table<T> table;
    private volatile int size;

    public LongObjectMap() {
        this(MIN_CAPACITY);
    }

    public LongObjectMap(int initialCapacity) {
        this.table = new Table<>(tableSizeFor(Math.max(MIN_CAPACITY, initialCapacity)));
    }

    // ---------------------------------------------------------------- 读（无锁）

    /**
     * 查询。乐观读，若期间发生写则降级为读锁重试。
     *
     * @return 对应 value；不存在返回 null
     */
    public T get(long key) {
        long stamp = lock.tryOptimisticRead();
        T v = find(table, key);
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                v = find(table, key);
            } finally {
                lock.unlockRead(stamp);
            }
        }
        return v;
    }

    public T getOrDefault(long key, T defaultValue) {
        T v = get(key);
        return v != null ? v : defaultValue;
    }

    public boolean containsKey(long key) {
        return get(key) != null;
    }

    /** 元素数量，最终一致（乐观读期间可能读到稍旧的值）。 */
    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    // ---------------------------------------------------------------- 写（互斥）

    /**
     * 写入键值对；若 key 已存在则覆盖旧值。
     *
     * @return 旧值；若之前不存在返回 null
     * @throws NullPointerException value 为 null
     */
    public T put(long key, T value) {
        Objects.requireNonNull(value, "value");
        long stamp = lock.writeLock();
        try {
            Table<T> t = table;
            int idx = spread(key) & t.mask;
            long[] ks = t.keys;
            Object[] vs = t.values;
            Object old;
            while ((old = vs[idx]) != null) {
                if (ks[idx] == key) {
                    vs[idx] = value;
                    return (T) old;
                }
                idx = (idx + 1) & t.mask;
            }
            ks[idx] = key;
            vs[idx] = value;
            onInsert(t);
            return null;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * 仅当 key 不存在时写入。
     *
     * @return 若已存在则返回旧值；否则写入并返回 null
     */
    public T putIfAbsent(long key, T value) {
        Objects.requireNonNull(value, "value");
        long stamp = lock.writeLock();
        try {
            Table<T> t = table;
            int idx = spread(key) & t.mask;
            long[] ks = t.keys;
            Object[] vs = t.values;
            Object old;
            while ((old = vs[idx]) != null) {
                if (ks[idx] == key) {
                    return (T) old;
                }
                idx = (idx + 1) & t.mask;
            }
            ks[idx] = key;
            vs[idx] = value;
            onInsert(t);
            return null;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /** 整体清空，保留当前容量。 */
    public void clear() {
        long stamp = lock.writeLock();
        try {
            table = new Table<>(table.keys.length);
            size = 0;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    // ---------------------------------------------------------------- 遍历

    /**
     * 遍历所有条目。回调参数为原始 long，无装箱。
     *
     * <p><b>警告</b>：遍历在读锁内进行，禁止在 consumer 内调用本 map 的写方法，
     * 否则 StampedLock 不可重入导致死锁。
     */
    public void forEach(EntryConsumer<T> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        long stamp = lock.readLock();
        try {
            Table<T> t = table;
            long[] ks = t.keys;
            Object[] vs = t.values;
            for (int i = 0; i < vs.length; i++) {
                Object v = vs[i];
                if (v != null) {
                    consumer.accept(ks[i], (T) v);
                }
            }
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @FunctionalInterface
    public interface EntryConsumer<T> {
        void accept(long key, T value);
    }

    // ---------------------------------------------------------------- 内部

    /** 插入成功后递增 size，必要时扩容。仅在写锁内调用。 */
    private void onInsert(Table<T> t) {
        int s = size + 1;
        size = s;
        if (s >= t.threshold) {
            grow(t);
        }
    }

    /** 纯读：在给定快照上查找。 */
    private static <T> T find(Table<T> t, long key) {
        int idx = spread(key) & t.mask;
        long[] ks = t.keys;
        Object[] vs = t.values;
        Object v;
        while ((v = vs[idx]) != null) {
            if (ks[idx] == key) {
                return (T) v;
            }
            idx = (idx + 1) & t.mask;
        }
        return null;
    }

    /** 扩容。仅在写锁内调用。 */
    private void grow(Table<T> old) {
        int oldCap = old.keys.length;
        if (oldCap >= MAX_CAPACITY) {
            return; // 已封顶，不再扩容
        }
        int newCap = oldCap << 1;
        if (newCap > MAX_CAPACITY || newCap <= 0) {
            newCap = MAX_CAPACITY;
        }
        Table<T> nt = new Table<>(newCap);
        for (int i = 0; i < oldCap; i++) {
            Object v = old.values[i];
            if (v != null) {
                long k = old.keys[i];
                int idx = spread(k) & nt.mask;
                while (nt.values[idx] != null) {
                    idx = (idx + 1) & nt.mask;
                }
                nt.keys[idx] = k;
                nt.values[idx] = v;
            }
        }
        table = nt; // volatile 发布；乐观读者若跨过此点，validate 会失败并重试
    }

    /** Murmur3 风格的 64→32 位混合，保证低位分布均匀。 */
    private static int spread(long key) {
        key ^= key >>> 32;
        key *= 0x9E3779B97F4A7C15L;
        key ^= key >>> 32;
        return (int) key;
    }

    /** 返回大于等于给定值的最小 2 的幂（上限 MAX_CAPACITY）。 */
    private static int tableSizeFor(int cap) {
        int n = -1 >>> Integer.numberOfLeadingZeros(cap - 1);
        if (n < 0) {
            return 1;
        }
        return Math.min(n + 1, MAX_CAPACITY);
    }
}