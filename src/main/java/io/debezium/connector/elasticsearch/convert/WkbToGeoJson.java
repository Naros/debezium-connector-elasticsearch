/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.convert;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal WKB reader producing GeoJSON structures for the seven standard geometry types, in XY
 * or XYZ form, accepting both byte orders and both ISO (+1000) and EWKB (flag bit) Z encodings.
 * Measured (M) coordinates are not supported and raise {@link IllegalArgumentException}.
 *
 * @author Chris Cranford
 */
final class WkbToGeoJson {

    private static final int EWKB_Z = 0x8000_0000;
    private static final int EWKB_M = 0x4000_0000;
    private static final int EWKB_SRID = 0x2000_0000;

    private WkbToGeoJson() {
    }

    static Map<String, Object> convert(byte[] wkb) {
        return readGeometry(ByteBuffer.wrap(wkb));
    }

    private static Map<String, Object> readGeometry(ByteBuffer buffer) {
        buffer.order(buffer.get() == 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        int type = buffer.getInt();

        boolean hasZ = (type & EWKB_Z) != 0;
        if ((type & EWKB_M) != 0) {
            throw new IllegalArgumentException("WKB geometries with M (measured) coordinates are not supported");
        }
        final boolean hasSrid = (type & EWKB_SRID) != 0;
        type &= 0x0FFF_FFFF;
        // ISO/SQL-MM encodes Z as +1000, M as +2000, and ZM as +3000.
        if (type >= 2000) {
            throw new IllegalArgumentException("WKB geometries with M (measured) coordinates are not supported");
        }
        if (type >= 1000) {
            hasZ = true;
            type -= 1000;
        }
        if (hasSrid) {
            buffer.getInt();
        }

        final int dims = hasZ ? 3 : 2;
        final Map<String, Object> geometry = new LinkedHashMap<>();
        switch (type) {
            case 1 -> {
                geometry.put("type", "Point");
                geometry.put("coordinates", readPosition(buffer, dims));
            }
            case 2 -> {
                geometry.put("type", "LineString");
                geometry.put("coordinates", readPositions(buffer, dims));
            }
            case 3 -> {
                geometry.put("type", "Polygon");
                geometry.put("coordinates", readRings(buffer, dims));
            }
            case 4 -> {
                geometry.put("type", "MultiPoint");
                geometry.put("coordinates", readSubGeometries(buffer, "Point"));
            }
            case 5 -> {
                geometry.put("type", "MultiLineString");
                geometry.put("coordinates", readSubGeometries(buffer, "LineString"));
            }
            case 6 -> {
                geometry.put("type", "MultiPolygon");
                geometry.put("coordinates", readSubGeometries(buffer, "Polygon"));
            }
            case 7 -> {
                final int count = buffer.getInt();
                final List<Object> geometries = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    geometries.add(readGeometry(buffer));
                }
                geometry.put("type", "GeometryCollection");
                geometry.put("geometries", geometries);
            }
            default -> throw new IllegalArgumentException("Unsupported WKB geometry type " + type);
        }
        return geometry;
    }

    private static List<Object> readSubGeometries(ByteBuffer buffer, String expectedType) {
        final int count = buffer.getInt();
        final List<Object> coordinates = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final Map<String, Object> sub = readGeometry(buffer);
            if (!expectedType.equals(sub.get("type"))) {
                throw new IllegalArgumentException(
                        "Expected nested " + expectedType + " but found " + sub.get("type"));
            }
            coordinates.add(sub.get("coordinates"));
        }
        return coordinates;
    }

    private static List<Object> readRings(ByteBuffer buffer, int dims) {
        final int count = buffer.getInt();
        final List<Object> rings = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rings.add(readPositions(buffer, dims));
        }
        return rings;
    }

    private static List<Object> readPositions(ByteBuffer buffer, int dims) {
        final int count = buffer.getInt();
        final List<Object> positions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            positions.add(readPosition(buffer, dims));
        }
        return positions;
    }

    private static List<Double> readPosition(ByteBuffer buffer, int dims) {
        final List<Double> position = new ArrayList<>(dims);
        for (int i = 0; i < dims; i++) {
            position.add(buffer.getDouble());
        }
        return position;
    }
}
