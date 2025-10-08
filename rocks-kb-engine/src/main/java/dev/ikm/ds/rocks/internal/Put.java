package dev.ikm.ds.rocks.internal;

import dev.ikm.ds.rocks.RocksProvider;
import dev.ikm.tinkar.component.Chronology;
import dev.ikm.tinkar.entity.*;

public class Put {

    public static void put(Chronology chronology) {
        Entity entity = EntityRecordFactory.make(chronology);
        if (entity instanceof SemanticEntity semanticEntity) {
            RocksProvider.get().merge(entity.nid(),
                    semanticEntity.patternNid(),
                    semanticEntity.referencedComponentNid(),
                    entity.getBytes(), semanticEntity);
        } else {
            RocksProvider.get().merge(entity.nid(), Integer.MAX_VALUE, Integer.MAX_VALUE, entity.getBytes(), entity);
        }
    }
}
