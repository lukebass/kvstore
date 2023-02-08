package com.s62023080.CPEN431.A4;

import java.util.Arrays;

public record Key(byte[] array) {

    @Override
    public byte[] array() {
        return array.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Key key = (Key) o;
        return Arrays.equals(this.array, key.array);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(array);
    }
}
