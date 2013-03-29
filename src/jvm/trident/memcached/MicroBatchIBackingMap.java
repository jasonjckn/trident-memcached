package trident.memcached;

import storm.trident.state.ValueUpdater;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MicroBatchIBackingMap<T> implements IBackingMap<T> {
    IBackingMap<T> _delegate;
    Options _options;


    public static class Options implements Serializable {
        public int maxMultiGetBatchSize = 0; // 0 means delegate batch size = trident batch size.
        public int maxMultiPutBatchSize = 0;
    }

    public MicroBatchIBackingMap(final Options options, final IBackingMap<T> delegate) {
        _options = options;
        _delegate = delegate;
        assert options.maxMultiPutBatchSize >= 0;
        assert options.maxMultiGetBatchSize >= 0;
    }

    @Override
    public void multiPut(final List<List<Object>> keys, final List<T> values) {
        int thisBatchSize;
        if(_options.maxMultiPutBatchSize == 0) { thisBatchSize = keys.size(); }
        else { thisBatchSize = _options.maxMultiPutBatchSize; }

        LinkedList<List<Object>> keysTodo = new LinkedList<List<Object>>(keys);
        LinkedList<T> valuesTodo = new LinkedList<T>(values);

        while(!keysTodo.isEmpty()) {
            List<List<Object>> keysBatch = new ArrayList<List<Object>>(thisBatchSize);
            List<T> valuesBatch = new ArrayList<T>(thisBatchSize);
            for(int i=0; i<thisBatchSize && !keysTodo.isEmpty(); i++) {
                keysBatch.add(keysTodo.removeFirst());
                valuesBatch.add(valuesTodo.removeFirst());
            }

            _delegate.multiPut(keysBatch, valuesBatch);
        }
    }

    @Override
    public List<T> multiGet(final List<List<Object>> keys) {
        int thisBatchSize;
        if(_options.maxMultiGetBatchSize == 0) { thisBatchSize = keys.size(); }
        else { thisBatchSize = _options.maxMultiGetBatchSize; }

        LinkedList<List<Object>> keysTodo = new LinkedList<List<Object>>(keys);

        List<T> ret = new ArrayList<T>(keys.size());

        while(!keysTodo.isEmpty()) {
            List<List<Object>> keysBatch = new ArrayList<List<Object>>(thisBatchSize);
            for(int i=0; i<thisBatchSize && !keysTodo.isEmpty(); i++) {
                keysBatch.add(keysTodo.removeFirst());
            }

            List<T> retSubset = _delegate.multiGet(keysBatch);
            ret.addAll(retSubset);
        }

        return ret;
    }
}
