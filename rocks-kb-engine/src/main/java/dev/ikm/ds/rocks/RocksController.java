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
        return RocksProvider.get().running();
    }

    @Override
    public void start() {
        RocksProvider.get();
    }

    @Override
    public void stop() {
        RocksProvider.get().close();
    }

    @Override
    public void save() {
        RocksProvider.get().save();
    }

    @Override
    public void reload() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveDataService provider() {
        return RocksProvider.get();
    }

    @Override
    public String toString() {
        return controllerName();
    }

}
