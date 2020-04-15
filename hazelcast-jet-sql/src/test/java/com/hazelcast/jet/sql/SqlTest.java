/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.impl.schema.JetSchema;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector.TO_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.schema.JetSchema.KAFKA_CONNECTOR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class SqlTest extends SimpleTestInClusterSupport {

    private static final String INT_TO_STRING_MAP_SRC = "int_to_string_map_src";
    private static final String INT_TO_STRING_MAP_SINK = "int_to_string_map_sink";

    private static final String PERSON_MAP_SRC = "person_map_src";
    private static final String PERSON_MAP_SINK = "person_map_sink";

    private static final String ALL_TYPES_MAP = "all_types_map";

    private static final Person PERSON_ALICE = new Person("Alice", 30);
    private static final Person PERSON_BOB = new Person("Bob", 40);
    private static final Person PERSON_CECILE = new Person("Cecile", 50);

    private static JetSqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        sqlService = new JetSqlService(instance());

        List<Entry<String, QueryDataType>> intToStringMapFields = asList(
                entry("__key", QueryDataType.INT),
                entry("this", QueryDataType.VARCHAR));
        sqlService.createTable(INT_TO_STRING_MAP_SRC, JetSchema.IMAP_LOCAL_SERVER,
                emptyMap(),
                intToStringMapFields);

        sqlService.createTable(INT_TO_STRING_MAP_SINK, JetSchema.IMAP_LOCAL_SERVER,
                emptyMap(),
                intToStringMapFields);

        List<Entry<String, QueryDataType>> personMapFields = asList(
                entry("__key", QueryDataType.INT),
                entry("name", QueryDataType.VARCHAR),
                entry("age", QueryDataType.INT));
        sqlService.createTable(PERSON_MAP_SRC, JetSchema.IMAP_LOCAL_SERVER,
                createMap(TO_VALUE_CLASS, Person.class.getName()),
                personMapFields);

        sqlService.createTable(PERSON_MAP_SINK, JetSchema.IMAP_LOCAL_SERVER,
                createMap(TO_VALUE_CLASS, Person.class.getName()),
                personMapFields);

        // an imap with a field of every type
        sqlService.createTable(ALL_TYPES_MAP, JetSchema.IMAP_LOCAL_SERVER,
                createMap(TO_VALUE_CLASS, AllTypesValue.class.getName()),
                asList(
                        entry("__key", QueryDataType.INT),
                        entry("string", QueryDataType.VARCHAR),
                        entry("character0", QueryDataType.VARCHAR_CHARACTER),
                        entry("character1", QueryDataType.VARCHAR_CHARACTER),
                        entry("boolean0", QueryDataType.BOOLEAN),
                        entry("boolean1", QueryDataType.BOOLEAN),
                        entry("byte0", QueryDataType.TINYINT),
                        entry("byte1", QueryDataType.TINYINT),
                        entry("short0", QueryDataType.SMALLINT),
                        entry("short1", QueryDataType.SMALLINT),
                        entry("int0", QueryDataType.INT),
                        entry("int1", QueryDataType.INT),
                        entry("long0", QueryDataType.BIGINT),
                        entry("long1", QueryDataType.BIGINT),
                        entry("bigDecimal", QueryDataType.DECIMAL),
                        entry("bigInteger", QueryDataType.DECIMAL_BIG_INTEGER),
                        entry("float0", QueryDataType.REAL),
                        entry("float1", QueryDataType.REAL),
                        entry("double0", QueryDataType.DOUBLE),
                        entry("double1", QueryDataType.DOUBLE),
                        entry("localTime", QueryDataType.TIME),
                        entry("localDate", QueryDataType.DATE),
                        entry("localDateTime", QueryDataType.TIMESTAMP),
                        entry("date", QueryDataType.TIMESTAMP_WITH_TZ_DATE),
                        entry("calendar", QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR),
                        entry("instant", QueryDataType.TIMESTAMP_WITH_TZ_INSTANT),
                        entry("zonedDateTime", QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME),
                        entry("offsetDateTime", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME),
                        entry("yearMonthInterval", QueryDataType.INTERVAL_YEAR_MONTH),
                        entry("daySecondInterval", QueryDataType.INTERVAL_DAY_SECOND)));
    }

    @Before
    public void before() {
        IMap<Integer, String> intToStringMap = instance().getMap(INT_TO_STRING_MAP_SRC);
        intToStringMap.put(0, "value-0");
        intToStringMap.put(1, "value-1");
        intToStringMap.put(2, "value-2");

        IMap<Integer, Person> personMap = instance().getMap(PERSON_MAP_SRC);
        personMap.put(0, PERSON_ALICE);
        personMap.put(1, PERSON_BOB);
        personMap.put(2, PERSON_CECILE);
    }

    @Test
    public void fullScan() throws Exception {
        assertRowsAnyOrder(
                "SELECT this, __key FROM " + INT_TO_STRING_MAP_SRC,
                asList(
                        new Row("value-0", 0),
                        new Row("value-1", 1),
                        new Row("value-2", 2)));
    }

    @Test
    public void fullScan_person() throws Exception {
        assertRowsAnyOrder(
                "SELECT __key, name, age FROM " + PERSON_MAP_SRC + " p",
                asList(
                        new Row(0, PERSON_ALICE.getName(), PERSON_ALICE.getAge()),
                        new Row(1, PERSON_BOB.getName(), PERSON_BOB.getAge()),
                        new Row(2, PERSON_CECILE.getName(), PERSON_CECILE.getAge())));
    }

    @Test
    public void fullScan_star() throws Exception {
        assertRowsAnyOrder(
                "SELECT * FROM " + INT_TO_STRING_MAP_SRC,
                asList(
                        new Row(0, "value-0"),
                        new Row(1, "value-1"),
                        new Row(2, "value-2")));
    }

    @Test
    public void fullScan_filter() throws Exception {
        assertRowsAnyOrder(
                "SELECT this FROM " + INT_TO_STRING_MAP_SRC + " WHERE __key=1 or this='value-0'",
                asList(new Row("value-1"), new Row("value-0")));
    }

    @Test
    public void fullScan_projection() throws Exception {
        assertRowsAnyOrder(
                "SELECT upper(this) FROM " + INT_TO_STRING_MAP_SRC + " WHERE this='value-1'",
                singletonList(new Row("VALUE-1")));
    }

    @Test
    public void fullScan_projection2() throws Exception {
        assertRowsAnyOrder(
                "SELECT this FROM " + INT_TO_STRING_MAP_SRC + " WHERE upper(this)='VALUE-1'",
                singletonList(new Row("value-1")));
    }

    @Test
    public void fullScan_projection3() throws Exception {
        assertRowsAnyOrder(
                "SELECT this FROM (SELECT upper(this) this FROM " + INT_TO_STRING_MAP_SRC + ") WHERE this='VALUE-1'",
                singletonList(new Row("VALUE-1")));
    }

    @Test
    public void fullScan_projection4() throws Exception {
        assertRowsAnyOrder(
                "SELECT upper(this) FROM " + INT_TO_STRING_MAP_SRC + " WHERE upper(this)='VALUE-1'",
                singletonList(new Row("VALUE-1")));
    }

    @Test
    public void selectWithoutFrom_unicode() throws Exception {
        assertRowsAnyOrder(
                "SELECT '喷气式飞机'",
                singletonList(new Row("喷气式飞机")));
    }

    @Test
    public void insert() {
        assertMap(
                INT_TO_STRING_MAP_SINK, "INSERT INTO " + INT_TO_STRING_MAP_SINK + " SELECT * FROM " + INT_TO_STRING_MAP_SRC,
                createMap(
                        0, "value-0",
                        1, "value-1",
                        2, "value-2"));
    }

    @Test
    public void insert_values() {
        assertMap(
                INT_TO_STRING_MAP_SINK, "INSERT INTO " + INT_TO_STRING_MAP_SINK + "(this, __key) values (2, 1)",
                createMap(1, "2"));
    }

    @Test
    public void insert_person() {
        assertMap(
                PERSON_MAP_SINK, "INSERT INTO " + PERSON_MAP_SINK + " VALUES (1, 'Foo', 25)",
                createMap(
                        1, new Person("Foo", 25)));
    }

    @Test
    public void stream_kafka() {
        sqlService.createServer("test", KAFKA_CONNECTOR, createMap(

        ));
    }

    @Test
    public void insert_allTypes() {
        assertMap(ALL_TYPES_MAP, "INSERT INTO " + ALL_TYPES_MAP + " VALUES (" +
                        "0, --key\n" +
                        "'string', --varchar\n" +
                        "'a', --character\n" +
                        "'b',\n" +
                        "true, --boolean\n" +
                        "false,\n" +
                        "126, --byte\n" +
                        "127, \n" +
                        "32766, --short\n" +
                        "32767, \n" +
                        "2147483646, --int \n" +
                        "2147483647,\n" +
                        "9223372036854775806, --long\n" +
                        "9223372036854775807,\n" +
                        // this is bigDecimal, but it's still limited to 64-bit unscaled value, see
                        // SqlValidatorImpl.validateLiteral()
                        "9223372036854775.123, --bigDecimal\n" +
                        "9223372036854775222, --bigInteger\n" +
                        "1234567890.1, --float\n" +
                        "1234567890.2, \n" +
                        "123451234567890.1, --double\n" +
                        "123451234567890.2,\n" +
                        "time'12:23:34', -- local time\n" +
                        "date'2020-04-15', -- local date \n" +
                        // there's no timestamp-with-tz literal in calcite apparently
                        "timestamp'2020-04-15 12:23:34.1', --timestamp\n" +
                        "timestamp'2020-04-15 12:23:34.2', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.3', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.4', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.5', --timestamp with tz\n" +
                        "timestamp'2020-04-15 12:23:34.6', --timestamp with tz\n" +
                        "INTERVAL '1' YEAR, -- year-to-month interval\n" +
                        "INTERVAL '1' HOUR -- day-to-second interval\n)",
                // TODO assert result
                createMap());
    }

    private <K, V> void assertMap(String mapName, String sql, Map<K, V> expected) {
        Job job = sqlService.execute(sql);
        job.join();
        assertEquals(expected, new HashMap<>(instance().getMap(mapName)));
    }

    private void assertRowsAnyOrder(String sql, Collection<Row> expectedRows) throws Exception {
        Observable<Object[]> observable = sqlService.executeQuery(sql);

        List<Object[]> result = observable.toFuture(str -> str.collect(toList())).get();
        assertEquals(new HashSet<>(expectedRows), result.stream().map(Row::new).collect(toSet()));
    }

    private static final class Row {
        Object[] values;

        Row(Object... values) {
            this.values = values;
        }

        @Override
        public String toString() {
            return "Row{" + Arrays.toString(values) + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Row row = (Row) o;
            return Arrays.equals(values, row.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }

    @SuppressWarnings("unused")
    public static final class Person implements Serializable {
        private String name;
        private int age;

        public Person() {
        }

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        @SuppressWarnings("unused") // used through reflection
        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        @SuppressWarnings("unused") // used through reflection
        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Person person = (Person) o;
            return age == person.age &&
                    Objects.equals(name, person.name);
        }

        @Override
        public String toString() {
            return "Person{name='" + name + '\'' + ", age=" + age + '}';
        }
    }
}
