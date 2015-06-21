package org.cloudysunny14.persistence.jpa.eclipselink;

import org.cloudysunny14.persistence.jpa.eclipselink.entity.User;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "persistence.JpaStoreUserEntityTest")
public class JpaEclipselinkStoreUserEntityTest extends BaseJpaEclipselinkStoreTest {
    @Override
    protected Class<?> getEntityClass() {
        return User.class;
    }

    @Override
    protected TestObject createTestObject(String suffix) {
        User user = new User();
        user.setUsername("u_" + suffix);
        user.setFirstName("fn_" + suffix);
        user.setLastName("ln_" + suffix);
        user.setNote("Some notes " + suffix);

        return new TestObject(user.getUsername(), user);
    }
}
