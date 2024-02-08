package com.github.bchapuis.calcite_postgis_tests;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.dialect.PostgisSqlDialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class SimplePostgisTest extends AbstractPostgisTest {

    public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(new PrecisionModel(), 4326);

    public static final Point POINT = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2));

    public static final LineString LINESTRING = GEOMETRY_FACTORY.createLineString(
            new Coordinate[]{
                    new Coordinate(0, 0), new Coordinate(1, 1), new Coordinate(2, 2)
            }
    );

    public static final Polygon POLYGON = GEOMETRY_FACTORY.createPolygon(
            GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
                    new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1), new Coordinate(0, 1), new Coordinate(0, 0)
            }),
            new LinearRing[]{
                    GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
                            new Coordinate(0.5, 0.5), new Coordinate(0.5, 0.6), new Coordinate(0.6, 0.6), new Coordinate(0.6, 0.5), new Coordinate(0.5, 0.5)
                    })
            }
    );

    public static final MultiPoint MULTIPOINT = GEOMETRY_FACTORY.createMultiPoint(
            new Point[]{
                    GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0)), GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1))
            }
    );

    public static final MultiLineString MULTILINESTRING = GEOMETRY_FACTORY.createMultiLineString(
            new LineString[]{
                    GEOMETRY_FACTORY.createLineString(new Coordinate[]{
                            new Coordinate(0, 0), new Coordinate(1, 1)
                    }),
                    GEOMETRY_FACTORY.createLineString(new Coordinate[]{
                            new Coordinate(2, 2), new Coordinate(3, 3)
                    })
            }
    );

    public static final MultiPolygon MULTIPOLYGON = GEOMETRY_FACTORY.createMultiPolygon(
            new Polygon[]{
                    GEOMETRY_FACTORY.createPolygon(
                            GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
                                    new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1), new Coordinate(0, 1), new Coordinate(0, 0)
                            })
                    ),
                    GEOMETRY_FACTORY.createPolygon(
                            GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
                                    new Coordinate(2, 2), new Coordinate(3, 2), new Coordinate(3, 3), new Coordinate(2, 3), new Coordinate(2, 2)
                            })
                    )
            }
    );

    public static final GeometryCollection GEOMETRY_COLLECTION = GEOMETRY_FACTORY.createGeometryCollection(
            new Geometry[]{
                    GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0)),
                    GEOMETRY_FACTORY.createLineString(new Coordinate[]{
                            new Coordinate(1, 1), new Coordinate(2, 2)
                    })
            }
    );

    @BeforeEach
    void setUp() throws SQLException {
        try (var connection = dataSource().getConnection()) {
            connection.createStatement().execute("CREATE TABLE points (id SERIAL PRIMARY KEY, geom GEOMETRY(POINT, 4326));");
            connection.createStatement().execute("CREATE TABLE linestrings (id SERIAL PRIMARY KEY, geom GEOMETRY(LINESTRING, 4326));");
            connection.createStatement().execute("CREATE TABLE polygons (id SERIAL PRIMARY KEY, geom GEOMETRY(POLYGON, 4326));");
            connection.createStatement().execute("CREATE TABLE multipoints (id SERIAL PRIMARY KEY, geom GEOMETRY(MULTIPOINT, 4326));");
            connection.createStatement().execute("CREATE TABLE multilinestrings (id SERIAL PRIMARY KEY, geom GEOMETRY(MULTILINESTRING, 4326));");
            connection.createStatement().execute("CREATE TABLE multipolygons (id SERIAL PRIMARY KEY, geom GEOMETRY(MULTIPOLYGON, 4326));");
            connection.createStatement().execute("CREATE TABLE geometrycollections (id SERIAL PRIMARY KEY, geom GEOMETRY(GEOMETRYCOLLECTION, 4326));");
            connection.createStatement().execute(String.format("INSERT INTO points (geom) VALUES (ST_GeomFromText('%s', %s));", POINT.toText(), POINT.getSRID()));
            connection.createStatement().execute(String.format("INSERT INTO linestrings (geom) VALUES (ST_GeomFromText('%s', %s));", LINESTRING.toText(), LINESTRING.getSRID()));
            connection.createStatement().execute(String.format("INSERT INTO polygons (geom) VALUES (ST_GeomFromText('%s', %s));", POLYGON.toText(), POLYGON.getSRID()));
            connection.createStatement().execute(String.format("INSERT INTO multipoints (geom) VALUES (ST_GeomFromText('%s', %s));", MULTIPOINT.toText(), MULTIPOINT.getSRID()));
            connection.createStatement().execute(String.format("INSERT INTO multilinestrings (geom) VALUES (ST_GeomFromText('%s', %s));", MULTILINESTRING.toText(), MULTILINESTRING.getSRID()));
            connection.createStatement().execute(String.format("INSERT INTO multipolygons (geom) VALUES (ST_GeomFromText('%s', %s));", MULTIPOLYGON.toText(), MULTIPOLYGON.getSRID()));
            connection.createStatement().execute(String.format("INSERT INTO geometrycollections (geom) VALUES (ST_GeomFromText('%s', %s));", GEOMETRY_COLLECTION.toText(), GEOMETRY_COLLECTION.getSRID()));
        }
    }

    public void decodeGeometry(String sql, Geometry expected) throws SQLException {
        Properties info = new Properties();
        info.setProperty("lex", "MYSQL");

        try (Connection connection = DriverManager.getConnection("jdbc:calcite:fun=spatial", info)) {
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            Schema schema = JdbcSchema.create(rootSchema, "geo", dataSource(), metaData -> PostgisSqlDialect.DEFAULT, null, "public");
            rootSchema.add("geo", schema);

            // Query the database
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                resultSet.next();
                var value = resultSet.getObject(1);
                assertEquals(expected, value);
            }
        }
    }

    @Test
    public void decodePoint() throws SQLException, ParseException {
        decodeGeometry("SELECT geom FROM geo.points", POINT);
    }

    @Test
    public void decodeBufferedPoint() throws SQLException, ParseException {
        decodeGeometry("SELECT st_buffer(geom, 1) FROM geo.points", POINT.buffer(1));
    }

    @Test
    public void decodeLineString() throws SQLException, ParseException {
        decodeGeometry("SELECT geom FROM geo.linestrings", LINESTRING);
    }

    @Test
    public void decodePolygon() throws SQLException, ParseException {
        decodeGeometry("SELECT geom FROM geo.polygons", POLYGON);
    }

    @Test
    public void decodeMultiPoint() throws SQLException, ParseException {
        decodeGeometry("SELECT geom FROM geo.multipoints", MULTIPOINT);
    }

    @Test
    public void decodeMultiLineString() throws SQLException, ParseException {
        decodeGeometry("SELECT geom FROM geo.multilinestrings", MULTILINESTRING);
    }

    @Test
    public void decodeMultiPolygon() throws SQLException, ParseException {
        decodeGeometry("SELECT geom FROM geo.multipolygons", MULTIPOLYGON);
    }

    @Test
    public void decodeGeometryCollection() throws SQLException, ParseException {
        decodeGeometry("SELECT geom FROM geo.geometrycollections", GEOMETRY_COLLECTION);
    }

}
