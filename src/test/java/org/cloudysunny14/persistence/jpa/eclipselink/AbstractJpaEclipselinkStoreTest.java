package org.cloudysunny14.persistence.jpa.eclipselink;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import org.cloudysunny14.persistence.jpa.eclipselink.configuration.JpaEclipselinkStoreConfigurationBuilder;
import org.eclipse.persistence.internal.jpa.EntityManagerFactoryImpl;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.DefaultTimeService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.JpaStoreTest")
public abstract class AbstractJpaEclipselinkStoreTest extends AbstractInfinispanTest {
    protected static final String PERSISTENCE_UNIT_NAME = "org.infinispan.persistence.jpa";

    protected EmbeddedCacheManager cm;

    protected AdvancedLoadWriteStore cs;

    //protected TransactionFactory gtf = new TransactionFactory();

    protected StreamingMarshaller marshaller;

    protected AbstractJpaEclipselinkStoreTest() {
        // gtf.init(false, false, true, false);
    }

    protected EmbeddedCacheManager createCacheManager() {
        return TestCacheManagerFactory.createCacheManager(true);
    }

    protected AdvancedLoadWriteStore createCacheStore() {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.persistence().addStore(JpaEclipselinkStoreConfigurationBuilder.class)
        .persistenceUnitName(PERSISTENCE_UNIT_NAME)
        .entityClass(getEntityClass());

        JpaEclipselinkStore store = new JpaEclipselinkStore();
        store.init(new InitializationContextImpl(builder.persistence().stores().get(0).create(), cm.getCache(),
                getMarshaller(), new DefaultTimeService(), null, new MarshalledEntryFactoryImpl(getMarshaller())));
        store.start();

        assertNotNull(store.getEntityManagerFactory());
        assertTrue(store.getEntityManagerFactory() instanceof EntityManagerFactoryImpl);

        return store;
    }

    protected abstract Class<?> getEntityClass();

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        try {
            cm = createCacheManager();
            marshaller = cm.getCache().getAdvancedCache().getComponentRegistry().getCacheMarshaller();
            cs = createCacheStore();
            cs.clear();
        } catch (Exception e) {
            System.err.println(e.toString());
            e.printStackTrace();
            throw e;
        }
    }

    @AfterMethod(alwaysRun = true)
    public void stopMarshaller() {
        //marshaller.stop();
        cs.stop();
        cm.stop();
    }

    /**
     * @return a mock marshaller for use with the cache store impls
     */
    protected StreamingMarshaller getMarshaller() {
        return marshaller;
    }

    protected MarshalledEntryImpl createEntry(Object key, Object value) {
        return new MarshalledEntryImpl(key, value, null, getMarshaller());
    }

    protected MarshalledEntryImpl createEntry(Object key, Object value, long lifespan) {
        InternalCacheEntry ice = TestInternalCacheEntryFactory.create(key, value, lifespan);
        return new MarshalledEntryImpl(key, value, new InternalMetadataImpl(ice), getMarshaller());
    }

    protected MarshalledEntryImpl createEntry(TestObject obj) {
        return createEntry(obj.getKey(), obj.getValue());
    }
}
