package org.cloudysunny14.persistence.jpa.eclipselink;

import org.cloudysunny14.persistence.jpa.eclipselink.configuration.JpaEclipselinkStoreConfigurationBuilder;
import org.cloudysunny14.persistence.jpa.eclipselink.entity.KeyValueEntity;
import org.cloudysunny14.persistence.jpa.eclipselink.impl.EntityManagerFactoryRegistry;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.JpaStoreTest")
public class JpaEclipselinkStoreTest extends BaseStoreTest {
    @Override
    protected AdvancedLoadWriteStore createStore() throws Exception {
        ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
        builder.persistence()
        .addStore(JpaEclipselinkStoreConfigurationBuilder.class)
        .persistenceUnitName(getPersistenceUnitName())
        .entityClass(KeyValueEntity.class)
        .storeMetadata(storeMetadata())
        .create();
        InitializationContext context = createContext(builder.build());
        context.getCache().getAdvancedCache().getComponentRegistry().getGlobalComponentRegistry()
        .registerComponent(new EntityManagerFactoryRegistry(), EntityManagerFactoryRegistry.class);
        JpaEclipselinkStore store = new JpaEclipselinkStore();
        store.init(context);
        return store;
    }

    protected boolean storeMetadata() {
        return true;
    }

    protected String getPersistenceUnitName() {
        return "org.infinispan.persistence.jpa";
    }

    @Override
    public Object wrap(String key, String value) {
        return new KeyValueEntity(key, value);
    }

    @Override
    public String unwrap(Object wrapper) {
        log.info("wrap" + wrapper);
        return ((KeyValueEntity) wrapper).getValue();
    }

    @Test(enabled = false)
    @Override
    public void testLoadAndStoreMarshalledValues() throws PersistenceException {
        // disabled as this test cannot be executed on JpaStore
    }
}
