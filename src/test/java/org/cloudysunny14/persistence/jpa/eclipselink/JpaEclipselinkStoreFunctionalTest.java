package org.cloudysunny14.persistence.jpa.eclipselink;

import java.lang.reflect.Method;

import org.cloudysunny14.persistence.jpa.eclipselink.configuration.JpaEclipselinkStoreConfigurationBuilder;
import org.cloudysunny14.persistence.jpa.eclipselink.entity.KeyValueEntity;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.spi.PersistenceException;
import org.testng.annotations.Test;

@Test(groups = {"unit", "smoke"}, testName = "persistence.JpaStoreFunctionalTest")
public class JpaEclipselinkStoreFunctionalTest extends BaseStoreFunctionalTest {
    @Override
    protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
        persistence.addStore(JpaEclipselinkStoreConfigurationBuilder.class)
        .persistenceUnitName("org.infinispan.persistence.jpa")
        .entityClass(KeyValueEntity.class)
        .preload(preload)
        .create();
        return persistence;
    }

    @Override
    public Object wrap(String key, String value) {
        return new KeyValueEntity(key, value);
    }

    @Override
    public String unwrap(Object wrapper) {
        return ((KeyValueEntity) wrapper).getValue();
    }

    @Test(enabled = false)
    @Override
    public void testTwoCachesSameCacheStore() {
        // With JPA, we must use different persistence configurations for each cache
        // as the database name is specified in persistence unit and table name
        // in the entity class.
    }

    @Test(enabled = false)
    public void testPreloadStoredAsBinary() {
        // byte arrays are not entities (no need to test how we can wrap them)
    }

    @Test(enabled = false)
    @Override
    public void testStoreByteArrays(Method m) throws PersistenceException {
        // byte arrays are not entities  (no need to test how we can wrap them)
    }

    @Test(enabled = false)
    @Override
    public void testRestoreAtomicMap(Method m) {
        // Atomic maps are not entities
    }

    @Test(enabled = false)
    @Override
    public void testRestoreTransactionalAtomicMap(Method m) throws Exception {
        // Atomic maps are not entities
    }
}
