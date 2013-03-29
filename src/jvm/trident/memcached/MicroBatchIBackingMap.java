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
        public int maxMultiGetBatchSize = 100;
        public int maxMultiPutBatchSize = 100;
    }

    public MicroBatchIBackingMap(final Options options, final IBackingMap<T> delegate) {
        _options = options;
        _delegate = delegate;
    }

    @Override
    public void multiPut(final List<List<Object>> keys, final List<T> values) {
        LinkedList<List<Object>> keysTodo = new LinkedList<List<Object>>(keys);
        LinkedList<T> valuesTodo = new LinkedList<T>(values);

        while(!keysTodo.isEmpty()) {
            List<List<Object>> keysBatch = new ArrayList<List<Object>>(_options.maxMultiPutBatchSize);
            List<T> valuesBatch = new ArrayList<T>(_options.maxMultiPutBatchSize);
            for(int i=0; i<_options.maxMultiPutBatchSize; i++) {
                keysBatch.add(keysTodo.removeFirst());
                valuesBatch.add(valuesTodo.removeFirst());
            }

            _delegate.multiPut(keysBatch, valuesBatch);
        }
    }

    @Override
    public List<T> multiGet(final List<List<Object>> keys) {
        LinkedList<List<Object>> keysTodo = new LinkedList<List<Object>>(keys);

        List<T> ret = new ArrayList<T>(keys.size());

        while(!keysTodo.isEmpty()) {
            List<List<Object>> keysBatch = new ArrayList<List<Object>>(_options.maxMultiGetBatchSize);
            for(int i=0; i<_options.maxMultiGetBatchSize; i++) {
                keysBatch.add(keysTodo.removeFirst());
            }

            List<T> retSubset = _delegate.multiGet(keysBatch);
            ret.addAll(retSubset);
        }

        return ret;
    }
}
