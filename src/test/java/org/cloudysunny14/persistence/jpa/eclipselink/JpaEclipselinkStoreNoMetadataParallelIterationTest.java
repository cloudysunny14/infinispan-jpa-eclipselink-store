package org.cloudysunny14.persistence.jpa.eclipselink;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.JpaStoreNoMetadataParallelIterationTest")
public class JpaEclipselinkStoreNoMetadataParallelIterationTest extends JpaEclipselinkStoreParallelIterationTest {
    @Override
    public String getPersistenceUnitName() {
        return "org.infinispan.persistence.jpa.no_metadata";
    }

    @Override
    protected boolean storeMetadata() {
        return false;
    }

    @Override
    protected boolean hasMetadata(int i) {
        return false;
    }
}
