package org.cloudysunny14.persistence.jpa.eclipselink.configuration;

import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class JpaEclipselinkStoreConfigurationBuilder 
extends AbstractStoreConfigurationBuilder<JpaEclipselinkStoreConfiguration, JpaEclipselinkStoreConfigurationBuilder> {

    private String persistenceUnitName;
    private Class<?> entityClass;
    private long batchSize = 100L;
    private boolean storeMetadata = true;

    public JpaEclipselinkStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
        super(builder);
    }

    public JpaEclipselinkStoreConfigurationBuilder persistenceUnitName(String persistenceUnitName) {
        this.persistenceUnitName = persistenceUnitName;
        return self();
    }

    public JpaEclipselinkStoreConfigurationBuilder entityClass(Class<?> entityClass) {
        this.entityClass = entityClass;
        return self();
    }

    public JpaEclipselinkStoreConfigurationBuilder batchSize(long batchSize) {
        this.batchSize = batchSize;
        return self();
    }

    public JpaEclipselinkStoreConfigurationBuilder storeMetadata(boolean storeMetadata) {
        this.storeMetadata = storeMetadata;
        return self();
    }

    @Override
    public void validate() {
        // how do you validate required attributes?
        super.validate();
    }

    @Override
    public JpaEclipselinkStoreConfiguration create() {
        return new JpaEclipselinkStoreConfiguration(
                purgeOnStartup, fetchPersistentState, ignoreModifications,
                async.create(), singletonStore.create(), preload, shared,
                TypedProperties.toTypedProperties(properties),
                persistenceUnitName, entityClass, batchSize, storeMetadata);
    }

    @Override
    public JpaEclipselinkStoreConfigurationBuilder read(JpaEclipselinkStoreConfiguration template) {
        persistenceUnitName = template.persistenceUnitName();
        entityClass = template.entityClass();
        batchSize = template.batchSize();
        storeMetadata = template.storeMetadata();

        fetchPersistentState = template.fetchPersistentState();
        ignoreModifications = template.ignoreModifications();
        properties = template.properties();
        purgeOnStartup = template.purgeOnStartup();
        async.read(template.async());
        singletonStore.read(template.singletonStore());
        preload = template.preload();
        shared = template.shared();
        return this;
    }

    @Override
    public JpaEclipselinkStoreConfigurationBuilder self() {
        return this;
    }
}