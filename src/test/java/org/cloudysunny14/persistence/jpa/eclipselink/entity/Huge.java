package org.cloudysunny14.persistence.jpa.eclipselink.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;

@Entity
public class Huge {
    @Id
    private String id;
    @Lob
    @Column(length = 1 << 20)
    private byte[] data;

    public Huge() {
    }

    public Huge(String id, byte[] data) {
        this.id = id;
        this.data = data;
    }
}
