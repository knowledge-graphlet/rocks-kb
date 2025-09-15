package dev.ikm.ds.rocks.internal;

import dev.ikm.ds.rocks.RocksProvider;
import dev.ikm.tinkar.component.Stamp;
import dev.ikm.tinkar.entity.ConceptEntity;
import dev.ikm.tinkar.entity.EntityRecordFactory;
import dev.ikm.tinkar.entity.StampEntity;
import org.eclipse.collections.api.list.ImmutableList;

import java.util.UUID;

public class Get {
    public static RocksProvider singleton;

    public static ConceptEntity concept(int nid) {
        return EntityRecordFactory.make(singleton.getBytes(nid));
    }

    public static StampEntity stamp(int nid) {
        return EntityRecordFactory.make(singleton.getBytes(nid));
    }

    public static int nidForUuids(ImmutableList<UUID> uuidList) {
        return singleton.nidForUuids(uuidList);
    }

    public static int stampNid(Stamp stamp) {
        throw new UnsupportedOperationException();
    }

    public static long sequenceForNid(int nid) {
        return singleton.elementSequenceForNid(nid);
    }
    public static int stampSequenceForStampNid(int stampNid) {
        return singleton.stampSequenceForStampNid(stampNid);
    }
}
