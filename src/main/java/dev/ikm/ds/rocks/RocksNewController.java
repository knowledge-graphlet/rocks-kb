package dev.ikm.ds.rocks;


import dev.ikm.ds.rocks.internal.Get;
import dev.ikm.tinkar.common.service.*;
import dev.ikm.tinkar.common.validation.ValidationRecord;
import dev.ikm.tinkar.common.validation.ValidationSeverity;
import dev.ikm.tinkar.entity.EntityCountSummary;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.MutableMap;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class RocksNewController extends RocksController {
    public static boolean loading = false;
    public static String CONTROLLER_NAME = "New Spined Array Store";
    public static final DataServiceProperty NEW_FOLDER_PROPERTY = new DataServiceProperty("New folder name", false, true);
    String importDataFileString;
    MutableMap<DataServiceProperty, String> providerProperties = Maps.mutable.empty();

    {
        providerProperties.put(NEW_FOLDER_PROPERTY, null);
    }

    @Override
    public ImmutableMap<DataServiceProperty, String> providerProperties() {
        return providerProperties.toImmutable();
    }

    @Override
    public void setDataServiceProperty(DataServiceProperty key, String value) {
        providerProperties.replace(key, value);
    }

    @Override
    public ValidationRecord[] validate(DataServiceProperty dataServiceProperty, Object value, Object target) {
        if (NEW_FOLDER_PROPERTY.equals(dataServiceProperty)) {
            File rootFolder = new File(System.getProperty("user.home"), "Solor");
            if (value instanceof String fileName) {
                if (fileName.isBlank()) {
                    return new ValidationRecord[]{new ValidationRecord(ValidationSeverity.ERROR,
                            "Directory name cannot be blank", target)};
                } else {
                    File possibleFile = new File(rootFolder, fileName);
                    if (possibleFile.exists()) {
                        return new ValidationRecord[]{new ValidationRecord(ValidationSeverity.ERROR,
                                "Directory already exists", target)};
                    }
                }
            }
        }
        return new ValidationRecord[]{};
    }

    public List<DataUriOption> providerOptions() {
        List<DataUriOption> dataUriOptions = new ArrayList<>();
        File rootFolder = new File(System.getProperty("user.home"), "Solor");
        if (!rootFolder.exists()) {
            rootFolder.mkdirs();
        }
        for (File f : rootFolder.listFiles()) {
            if (isValidDataLocation(f.getName())) {
                dataUriOptions.add(new DataUriOption(f.getName(), f.toURI()));
            }
        }
        return dataUriOptions;
    }

    @Override
    public boolean isValidDataLocation(String name) {
        return name.toLowerCase().endsWith(".zip") && name.toLowerCase().contains("tink");
    }

    @Override
    public void setDataUriOption(DataUriOption option) {
        try {
            importDataFileString = option.uri().toURL().getFile();
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String controllerName() {
        return CONTROLLER_NAME;
    }

    @Override
    public void start() {
        if (RocksProvider.singleton == null) {
            RocksNewController.loading = true;
            try {
                //File rootFolder = new File(System.getProperty("user.home"), "Solor");
                File rootFolder = new File("target");
                File dataDirectory = new File(rootFolder, providerProperties.get(NEW_FOLDER_PROPERTY));
                ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, dataDirectory);
                RocksProvider rocksProvider = new RocksProvider();

//                ServiceLoader<LoadDataFromFileController> controllerFinder = PluggableService.load(LoadDataFromFileController.class);
//                LoadDataFromFileController loader = controllerFinder.findFirst().get();
//                Future<EntityCountSummary> loadFuture = (Future<EntityCountSummary>) loader.load(new File(importDataFileString));
//                EntityCountSummary count = loadFuture.get();
//                Get.singleton.save();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            RocksNewController.loading = false;
        }
    }

    @Override
    public boolean loading() {
        return RocksNewController.loading;
    }
}
