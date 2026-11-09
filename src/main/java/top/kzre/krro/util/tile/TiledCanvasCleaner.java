package top.kzre.krro.util.tile;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link TiledCanvas} 的兜底资源清理器。
 *
 * <p>调用 {@link TiledCanvas#close()} 是首选释放方式；本类只在画布被 GC 回收
 * 但没 close 时兜底释放 {@link TileData} 引用。两条路径共用
 * {@link Registration#cleanup()}，由其幂等性保证不会重复释放。
 *
 * <p><b>设计约束</b>：{@link Registration} 严禁直接或间接强引用
 * {@link TiledCanvas}——否则 referent 永远不可达，本清理器失效。
 * 这也是 {@link AbstractTile#attach(TiledCanvas)} 必须用
 * {@link java.lang.ref.WeakReference} 保存 owner 的原因。
 */
final class TiledCanvasCleaner {

    private TiledCanvasCleaner() {}

    private static final ReferenceQueue<TiledCanvas> QUEUE = new ReferenceQueue<>();

    /**
     * 强引用所有尚未 cleanup 的 Registration。PhantomReference 本身是弱引用，
     * 如果不被外部强引用，会在 referent 被回收之前就被 GC 掉，导致队列里什么都收不到。
     */
    private static final Set<Registration> PENDING =
            Collections.synchronizedSet(new HashSet<>());

    static {
        Thread t = new Thread(TiledCanvasCleaner::drainLoop, "tiledcanvas-cleaner");
        t.setDaemon(true);
        t.start();
    }

    private static void drainLoop() {
        while (true) {
            try {
                Reference<? extends TiledCanvas> ref = QUEUE.remove();
                Registration reg = (Registration) ref;
                PENDING.remove(reg);
                reg.cleanup();
            } catch (InterruptedException e) {
                return;
            } catch (Throwable ex) {
                // 兜底线程绝不能死——吞掉异常继续 drain
                System.err.println("[TiledCanvasCleaner] " + ex.getMessage());
                ex.printStackTrace(System.err);
            }
        }
    }

    /**
     * 注册清理动作。返回的 Registration 由 TiledCanvas 持有，用于 close() 时主动触发。
     *
     * @param canvas 只作 phantom referent，本方法不会保留对它的强引用
     * @param tiles  待释放的瓦片容器；必须是 canvas 的同一实例
     */
    static Registration register(TiledCanvas canvas, ConcurrentHashMap<Long, Tile> tiles) {
        Registration reg = new Registration(canvas, tiles);
        PENDING.add(reg);
        return reg;
    }

    /**
     * 每个 {@link TiledCanvas} 一份。只持有 {@code tiles} 容器，
     * 不持有 canvas 本身，也不持有任何可能反向引用 canvas 的对象。
     */
    static final class Registration extends PhantomReference<TiledCanvas> {

        private final ConcurrentHashMap<Long, Tile> tiles;
        private final AtomicBoolean done = new AtomicBoolean(false);

        private Registration(TiledCanvas referent, ConcurrentHashMap<Long, Tile> tiles) {
            super(referent, QUEUE);
            this.tiles = tiles;
        }

        /**
         * 逐个 release {@link Tile#getDataRef()} 并清空容器。
         * 幂等：只有第一次调用生效，无论是 close() 主动调用还是 cleaner 线程兜底调用。
         */
        void cleanup() {
            if (!done.compareAndSet(false, true)) return;

            for (Map.Entry<Long, Tile> entry : tiles.entrySet()) {
                try {
                    entry.getValue().getDataRef().release();
                } catch (Throwable ex) {
                    System.err.println("[TiledCanvasCleaner] release failed: " + ex.getMessage());
                }
            }
            tiles.clear();

            // 解除 phantom 引用：此后再无人通过本 Registration 观察 referent
            clear();
            PENDING.remove(this);
        }
    }
}