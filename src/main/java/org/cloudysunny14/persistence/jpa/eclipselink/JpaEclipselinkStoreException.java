package org.cloudysunny14.persistence.jpa.eclipselink;

import org.infinispan.persistence.spi.PersistenceException;

public class JpaEclipselinkStoreException extends PersistenceException {
    public JpaEclipselinkStoreException() {
        super();
    }

    public JpaEclipselinkStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public JpaEclipselinkStoreException(String message) {
        super(message);
    }

    public JpaEclipselinkStoreException(Throwable cause) {
        super(cause);
    }
}
