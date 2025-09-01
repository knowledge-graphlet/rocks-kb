package dev.ikm.ds.rocks;

import dev.ikm.tinkar.common.service.DataUriOption;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;

public class RocksOpenController extends RocksController {
    public static String CONTROLLER_NAME = "Open RocksDB";

    @Override
    public boolean isValidDataLocation(String name) {
        return name.equals("rocksdb");
    }

    @Override
    public void setDataUriOption(DataUriOption option) {
        ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, option.toFile());
    }

    @Override
    public String controllerName() {
        return CONTROLLER_NAME;
    }

}
