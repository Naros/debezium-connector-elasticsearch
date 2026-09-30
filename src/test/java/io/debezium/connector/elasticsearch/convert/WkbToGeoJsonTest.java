/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.convert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers the seven WKB geometry types in both byte orders, the ISO and EWKB Z encodings, the
 * SRID flag, and the rejection of measured coordinates.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class WkbToGeoJsonTest {

    private static final int EWKB_Z = 0x8000_0000;
    private static final int EWKB_M = 0x4000_0000;
    private static final int EWKB_SRID = 0x2000_0000;

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldReadPointInEitherByteOrder(boolean littleEndian) {
        final byte[] wkb = geometry(littleEndian, 1, null, 1.5, 2.5);
        assertThat(WkbToGeoJson.convert(wkb)).isEqualTo(Map.of("type", "Point", "coordinates", List.of(1.5, 2.5)));
    }

    @Test
    void shouldReadLineStringAndPolygon() {
        final byte[] line = geometry(true, 2, null, 2, 0.0, 0.0, 1.0, 1.0);
        assertThat(WkbToGeoJson.convert(line))
                .isEqualTo(Map.of("type", "LineString", "coordinates", List.of(List.of(0.0, 0.0), List.of(1.0, 1.0))));

        final byte[] polygon = geometry(true, 3, null, 1, 3, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0);
        assertThat(WkbToGeoJson.convert(polygon))
                .isEqualTo(Map.of("type", "Polygon", "coordinates",
                        List.of(List.of(List.of(0.0, 0.0), List.of(1.0, 0.0), List.of(0.0, 0.0)))));
    }

    @Test
    void shouldReadMultiGeometriesAndCollections() {
        final byte[] point = geometry(true, 1, null, 1.5, 2.5);
        final byte[] line = geometry(false, 2, null, 1, 0.0, 0.0);
        final byte[] polygon = geometry(true, 3, null, 1, 1, 0.0, 0.0);

        assertThat(WkbToGeoJson.convert(collection(true, 4, point, point)))
                .isEqualTo(Map.of("type", "MultiPoint", "coordinates", List.of(List.of(1.5, 2.5), List.of(1.5, 2.5))));
        assertThat(WkbToGeoJson.convert(collection(false, 5, line)))
                .isEqualTo(Map.of("type", "MultiLineString", "coordinates", List.of(List.of(List.of(0.0, 0.0)))));
        assertThat(WkbToGeoJson.convert(collection(true, 6, polygon)))
                .isEqualTo(Map.of("type", "MultiPolygon", "coordinates", List.of(List.of(List.of(List.of(0.0, 0.0))))));
        assertThat(WkbToGeoJson.convert(collection(true, 7, point, line)))
                .containsEntry("type", "GeometryCollection")
                .extracting("geometries").asInstanceOf(InstanceOfAssertFactories.LIST).hasSize(2);
    }

    @Test
    void shouldReadZCoordinatesUnderIsoAndEwkbEncodings() {
        final Map<String, Object> expected = Map.of("type", "Point", "coordinates", List.of(1.0, 2.0, 3.0));
        assertThat(WkbToGeoJson.convert(geometry(true, 1001, null, 1.0, 2.0, 3.0))).isEqualTo(expected);
        assertThat(WkbToGeoJson.convert(geometry(true, 1 | EWKB_Z, null, 1.0, 2.0, 3.0))).isEqualTo(expected);
        assertThat(WkbToGeoJson.convert(geometry(true, 1 | EWKB_Z | EWKB_SRID, 4326, 1.0, 2.0, 3.0))).isEqualTo(expected);
    }

    @Test
    void shouldSkipEwkbSridWithoutZ() {
        assertThat(WkbToGeoJson.convert(geometry(true, 1 | EWKB_SRID, 4326, 1.0, 2.0)))
                .isEqualTo(Map.of("type", "Point", "coordinates", List.of(1.0, 2.0)));
    }

    @Test
    void shouldRejectMeasuredCoordinatesAndUnknownTypes() {
        assertThatThrownBy(() -> WkbToGeoJson.convert(geometry(true, 1 | EWKB_M, null, 1.0, 2.0, 3.0)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("measured");
        assertThatThrownBy(() -> WkbToGeoJson.convert(geometry(true, 2001, null, 1.0, 2.0, 3.0)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("measured");
        assertThatThrownBy(() -> WkbToGeoJson.convert(geometry(true, 3001, null, 1.0, 2.0, 3.0, 4.0)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("measured");
        assertThatThrownBy(() -> WkbToGeoJson.convert(geometry(true, 8, null)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("type 8");
    }

    @Test
    void shouldRejectMismatchedNestedGeometryType() {
        final byte[] line = geometry(true, 2, null, 1, 0.0, 0.0);
        assertThatThrownBy(() -> WkbToGeoJson.convert(collection(true, 4, line)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected nested Point");
    }

    /**
     * Encodes a header (byte order, type, optional SRID) followed by the given body, where each
     * body element is written as an int when it is an Integer and as a double otherwise.
     */
    private static byte[] geometry(boolean littleEndian, int type, Integer srid, Number... body) {
        final ByteBuffer buffer = ByteBuffer.allocate(13 + body.length * 8).order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
        buffer.put((byte) (littleEndian ? 1 : 0)).putInt(type);
        if (srid != null) {
            buffer.putInt(srid);
        }
        for (Number value : body) {
            if (value instanceof Integer count) {
                buffer.putInt(count);
            }
            else {
                buffer.putDouble(value.doubleValue());
            }
        }
        final byte[] result = new byte[buffer.position()];
        buffer.flip();
        buffer.get(result);
        return result;
    }

    private static byte[] collection(boolean littleEndian, int type, byte[]... members) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(geometry(littleEndian, type, null, members.length));
        for (byte[] member : members) {
            out.writeBytes(member);
        }
        return out.toByteArray();
    }
}
