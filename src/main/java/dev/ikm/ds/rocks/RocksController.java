package dev.ikm.ds.rocks;

import dev.ikm.tinkar.common.service.DataServiceController;
import dev.ikm.tinkar.common.service.PrimitiveDataService;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public abstract class RocksController implements DataServiceController<PrimitiveDataService> {

    @Override
    public Class<? extends PrimitiveDataService> serviceClass() {
        return PrimitiveDataService.class;
    }

    @Override
    public boolean running() {
        return RocksProvider.singleton != null;
    }

    @Override
    public void start() {
        try {
            new RocksProvider();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        if (RocksProvider.singleton != null) {
            RocksProvider.singleton.close();
            RocksProvider.singleton = null;
        }
    }

    @Override
    public void save() {
        if (RocksProvider.singleton != null) {
            RocksProvider.singleton.save();
        }
    }

    @Override
    public void reload() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveDataService provider() {
        if (RocksProvider.singleton == null) {
            start();
        }
        return RocksProvider.singleton;
    }

    @Override
    public String toString() {
        return controllerName();
    }

}
