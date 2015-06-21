package org.cloudysunny14.persistence.jpa.eclipselink;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;

import org.cloudysunny14.persistence.jpa.eclipselink.configuration.JpaEclipselinkStoreConfiguration;
import org.cloudysunny14.persistence.jpa.eclipselink.configuration.JpaEclipselinkStoreConfigurationBuilder;
import org.cloudysunny14.persistence.jpa.eclipselink.entity.User;
import org.cloudysunny14.persistence.jpa.eclipselink.entity.Vehicle;
import org.cloudysunny14.persistence.jpa.eclipselink.entity.VehicleId;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.JpaConfigurationTest")
public class JpaEclipselinkConfigurationTest {

    private static final String PERSISTENCE_UNIT_NAME = "org.infinispan.persistence.jpa.configurationTest";

    public void testConfigBuilder() {
        GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
        .globalJmxStatistics().allowDuplicateDomains(true).transport().defaultTransport().build();

        Configuration cacheConfig = new ConfigurationBuilder().persistence()
                .addStore(JpaEclipselinkStoreConfigurationBuilder.class)
                .persistenceUnitName(PERSISTENCE_UNIT_NAME)
                .entityClass(User.class).build();


        StoreConfiguration storeConfiguration = cacheConfig.persistence().stores().get(0);
        assertTrue(storeConfiguration instanceof JpaEclipselinkStoreConfiguration);
        JpaEclipselinkStoreConfiguration jpaCacheLoaderConfig = (JpaEclipselinkStoreConfiguration) storeConfiguration;
        assertEquals(PERSISTENCE_UNIT_NAME, jpaCacheLoaderConfig.persistenceUnitName());
        assertEquals(User.class, jpaCacheLoaderConfig.entityClass());

        EmbeddedCacheManager cacheManager = new DefaultCacheManager(globalConfig);

        cacheManager.defineConfiguration("userCache", cacheConfig);

        cacheManager.start();
        Cache<String, User> userCache = cacheManager.getCache("userCache");
        User user = new User();
        user.setUsername("rtsang");
        user.setFirstName("Ray");
        user.setLastName("Tsang");
        userCache.put(user.getUsername(), user);
        userCache.stop();
        cacheManager.stop();
    }

    protected void validateConfig(Cache<VehicleId, Vehicle> vehicleCache) {
        StoreConfiguration config = vehicleCache.getCacheConfiguration().persistence().stores().get(0);

        assertTrue(config instanceof JpaEclipselinkStoreConfiguration);
        JpaEclipselinkStoreConfiguration jpaConfig = (JpaEclipselinkStoreConfiguration) config;
        assertEquals(1, jpaConfig.batchSize());
        assertEquals(Vehicle.class, jpaConfig.entityClass());
        assertEquals(PERSISTENCE_UNIT_NAME, jpaConfig.persistenceUnitName());
    }

    public void testXmlConfig() throws IOException {
        EmbeddedCacheManager cacheManager = new DefaultCacheManager("config/jpa-config.xml");

        Cache<VehicleId, Vehicle> vehicleCache = cacheManager.getCache("vehicleCache");
        validateConfig(vehicleCache);

        Vehicle v = new Vehicle();
        v.setId(new VehicleId("NC", "123456"));
        v.setColor("BLUE");
        vehicleCache.put(v.getId(), v);

        vehicleCache.stop();
        cacheManager.stop();
    }
}
