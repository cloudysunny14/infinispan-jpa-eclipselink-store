package org.cloudysunny14.persistence.jpa.eclipselink;

public class TestObject {
    private Object key;
    private Object value;

    public TestObject(Object key, Object value) {
        super();
        this.key = key;
        this.value = value;
    }

    public Object getKey() {
        return key;
    }
    public void setKey(Object key) {
        this.key = key;
    }
    public Object getValue() {
        return value;
    }
    public void setValue(Object value) {
        this.value = value;
    }
}
