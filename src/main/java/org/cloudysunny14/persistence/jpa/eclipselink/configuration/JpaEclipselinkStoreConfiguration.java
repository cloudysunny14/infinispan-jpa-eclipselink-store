package org.cloudysunny14.persistence.jpa.eclipselink.configuration;

import java.util.Properties;

import org.cloudysunny14.persistence.jpa.eclipselink.JpaEclipselinkStore;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;

@BuiltBy(JpaEclipselinkStoreConfigurationBuilder.class)
@ConfigurationFor(JpaEclipselinkStore.class)
public class JpaEclipselinkStoreConfiguration 
extends AbstractStoreConfiguration {

    final private String persistenceUnitName;
    final private Class<?> entityClass;
    final private long batchSize;
    final private boolean storeMetadata;

    public JpaEclipselinkStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState,
            boolean ignoreModifications, AsyncStoreConfiguration async,
            SingletonStoreConfiguration singletonStore, boolean preload, boolean shared,
            Properties properties,
            String persistenceUnitName, Class<?> entityClass,
            long batchSize, boolean storeMetadata) {
        super(purgeOnStartup, fetchPersistentState, ignoreModifications, async,
                singletonStore, preload, shared, properties);
        this.persistenceUnitName = persistenceUnitName;
        this.entityClass = entityClass;
        this.batchSize = batchSize;
        this.storeMetadata = storeMetadata;
    }

    public String persistenceUnitName() {
        return persistenceUnitName;
    }

    public Class<?> entityClass() {
        return entityClass;
    }

    public long batchSize() {
        return batchSize;
    }

    public boolean storeMetadata() {
        return storeMetadata;
    }
}