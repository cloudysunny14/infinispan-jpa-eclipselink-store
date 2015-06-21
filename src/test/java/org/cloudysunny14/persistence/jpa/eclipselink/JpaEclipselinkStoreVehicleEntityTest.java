package org.cloudysunny14.persistence.jpa.eclipselink;

import org.cloudysunny14.persistence.jpa.eclipselink.entity.Vehicle;
import org.cloudysunny14.persistence.jpa.eclipselink.entity.VehicleId;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "persistence.JpaStoreVehicleEntityTest")
public class JpaEclipselinkStoreVehicleEntityTest extends BaseJpaEclipselinkStoreTest {
    @Override
    protected Class<?> getEntityClass() {
        return Vehicle.class;
    }

    @Override
    protected TestObject createTestObject(String key) {
        VehicleId id = new VehicleId("CA" + key, key);
        Vehicle v = new Vehicle();
        v.setId(id);
        v.setColor("c_" + key);

        return new TestObject(v.getId(), v);
    }
}
