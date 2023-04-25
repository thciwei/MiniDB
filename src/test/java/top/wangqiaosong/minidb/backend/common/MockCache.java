package top.wangqiaosong.minidb.backend.common;

public class MockCache extends AbstractCache<Long> {

    public MockCache() {
        /**
         * this只能调用父类方法，super调用父类构造器
         */
        super(50);
    }

    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long obj) {
    }

}
