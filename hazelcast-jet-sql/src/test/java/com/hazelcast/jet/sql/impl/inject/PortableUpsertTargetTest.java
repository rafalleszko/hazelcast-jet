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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PortableUpsertTargetTest {

    @Test
    public void test_set() throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        ClassDefinition innerClassDefinition = new ClassDefinitionBuilder(4, 5, 6).build();
        ss.getPortableContext().registerClassDefinition(innerClassDefinition);

        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addPortableField("null", innerClassDefinition)
                        .addPortableField("object", innerClassDefinition)
                        .addUTFField("string")
                        .addCharField("character")
                        .addBooleanField("boolean")
                        .addByteField("byte")
                        .addShortField("short")
                        .addIntField("int")
                        .addLongField("long")
                        .addFloatField("float")
                        .addDoubleField("double")
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);

        UpsertTarget target = new PortableUpsertTarget(
                ss,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()
        );
        UpsertInjector nullFieldInjector = target.createInjector("null", QueryDataType.OBJECT);
        UpsertInjector objectFieldInjector = target.createInjector("object", QueryDataType.OBJECT);
        UpsertInjector stringFieldInjector = target.createInjector("string", QueryDataType.VARCHAR);
        UpsertInjector characterFieldInjector = target.createInjector("character", QueryDataType.VARCHAR_CHARACTER);
        UpsertInjector booleanFieldInjector = target.createInjector("boolean", QueryDataType.BOOLEAN);
        UpsertInjector byteFieldInjector = target.createInjector("byte", QueryDataType.TINYINT);
        UpsertInjector shortFieldInjector = target.createInjector("short", QueryDataType.SMALLINT);
        UpsertInjector intFieldInjector = target.createInjector("int", QueryDataType.INT);
        UpsertInjector longFieldInjector = target.createInjector("long", QueryDataType.BIGINT);
        UpsertInjector floatFieldInjector = target.createInjector("float", QueryDataType.REAL);
        UpsertInjector doubleFieldInjector = target.createInjector("double", QueryDataType.DOUBLE);

        target.init();
        nullFieldInjector.set(null);
        objectFieldInjector.set(new InnerPortable());
        stringFieldInjector.set("1");
        characterFieldInjector.set('2');
        booleanFieldInjector.set(true);
        byteFieldInjector.set((byte) 3);
        shortFieldInjector.set((short) 4);
        intFieldInjector.set(5);
        longFieldInjector.set(6L);
        floatFieldInjector.set(7.1F);
        doubleFieldInjector.set(7.2D);
        Object portable = target.conclude();

        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(portable));
        assertThat(record.readGenericRecord("null")).isNull();
        assertThat(record.readGenericRecord("object")).isNotNull();
        assertThat(record.readUTF("string")).isEqualTo("1");
        assertThat(record.readChar("character")).isEqualTo('2');
        assertThat(record.readBoolean("boolean")).isEqualTo(true);
        assertThat(record.readByte("byte")).isEqualTo((byte) 3);
        assertThat(record.readShort("short")).isEqualTo((short) 4);
        assertThat(record.readInt("int")).isEqualTo(5);
        assertThat(record.readLong("long")).isEqualTo(6L);
        assertThat(record.readFloat("float")).isEqualTo(7.1F);
        assertThat(record.readDouble("double")).isEqualTo(7.2D);
    }

    @Test
    public void when_injectNonExistingPropertyValue_then_throws() {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);

        UpsertTarget target = new PortableUpsertTarget(
                ss,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()
        );
        UpsertInjector injector = target.createInjector("field", QueryDataType.INT);

        target.init();
        assertThatThrownBy(() -> injector.set("1"))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Unable to inject a non-null value to 'field'");
    }

    @Test
    public void when_injectNonExistingPropertyNullValue_then_succeeds() throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);

        UpsertTarget target = new PortableUpsertTarget(
                ss,
                classDefinition.getFactoryId(), classDefinition.getClassId(), classDefinition.getVersion()
        );
        UpsertInjector injector = target.createInjector("field", QueryDataType.INT);

        target.init();
        injector.set(null);
        Object portable = target.conclude();

        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(portable));
        assertThat(record).isNotNull();
    }

    private static final class InnerPortable implements Portable {

        @Override
        public int getFactoryId() {
            return 4;
        }

        @Override
        public int getClassId() {
            return 5;
        }

        @Override
        public void writePortable(PortableWriter writer) {
        }

        @Override
        public void readPortable(PortableReader reader) {
        }
    }
}
