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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.connector.EntryMetadata;
import com.hazelcast.jet.sql.impl.connector.EntryMetadataResolver;
import com.hazelcast.jet.sql.impl.inject.PortableUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.sql.SqlConnector.PORTABLE_SERIALIZATION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.EntrySqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.ResolverUtil.lookupClassDefinition;

// TODO: deduplicate with MapSampleMetadataResolver
public final class PortableEntryMetadataResolver implements EntryMetadataResolver {

    public static final PortableEntryMetadataResolver INSTANCE = new PortableEntryMetadataResolver();

    private PortableEntryMetadataResolver() {
    }

    @Override
    public String supportedFormat() {
        return PORTABLE_SERIALIZATION_FORMAT;
    }

    @Override
    public List<MappingField> resolveFields(
            List<MappingField> mappingFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        ClassDefinition classDefinition = resolveClassDefinition(isKey, options, serializationService);

        Map<QueryPath, MappingField> externalFieldsByPath = isKey
                ? extractKeyFields(mappingFields)
                : extractValueFields(mappingFields, name -> new QueryPath(name, false));

        Map<String, MappingField> fields = new LinkedHashMap<>();
        for (Entry<String, FieldType> entry : resolvePortable(classDefinition).entrySet()) {
            QueryPath path = new QueryPath(entry.getKey(), isKey);
            QueryDataType type = resolvePortableType(entry.getValue());

            MappingField mappingField = externalFieldsByPath.get(path);
            if (mappingField != null && !type.getTypeFamily().equals(mappingField.type().getTypeFamily())) {
                throw QueryException.error("Mismatch between declared and inferred type - '" + mappingField.name() + "'");
            }
            String name = mappingField == null ? entry.getKey() : mappingField.name();

            MappingField field = new MappingField(name, type, path.toString());

            fields.putIfAbsent(field.name(), field);
        }
        for (Entry<QueryPath, MappingField> entry : externalFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            String name = entry.getValue().name();
            QueryDataType type = entry.getValue().type();

            MappingField field = new MappingField(name, type, path.toString());

            fields.putIfAbsent(field.name(), field);
        }
        return new ArrayList<>(fields.values());
    }

    private static Map<String, FieldType> resolvePortable(ClassDefinition classDefinition) {
        Map<String, FieldType> fields = new LinkedHashMap<>();
        for (int i = 0; i < classDefinition.getFieldCount(); i++) {
            FieldDefinition fieldDefinition = classDefinition.getField(i);
            fields.putIfAbsent(fieldDefinition.getName(), fieldDefinition.getType());
        }
        return fields;
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static QueryDataType resolvePortableType(FieldType portableType) {
        switch (portableType) {
            case BOOLEAN:
                return QueryDataType.BOOLEAN;
            case BYTE:
                return QueryDataType.TINYINT;
            case SHORT:
                return QueryDataType.SMALLINT;
            case CHAR:
                return QueryDataType.VARCHAR_CHARACTER;
            case UTF:
                return QueryDataType.VARCHAR;
            case INT:
                return QueryDataType.INT;
            case LONG:
                return QueryDataType.BIGINT;
            case FLOAT:
                return QueryDataType.REAL;
            case DOUBLE:
                return QueryDataType.DOUBLE;
            default:
                return QueryDataType.OBJECT;
        }
    }

    @Override
    public EntryMetadata resolveMetadata(
            List<MappingField> mappingFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        ClassDefinition classDefinition = resolveClassDefinition(isKey, options, serializationService);

        Map<QueryPath, MappingField> externalFieldsByPath = isKey
                ? extractKeyFields(mappingFields)
                : extractValueFields(mappingFields, name -> new QueryPath(name, false));

        List<TableField> fields = new ArrayList<>();
        for (Entry<QueryPath, MappingField> entry : externalFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            TableField field = new MapTableField(name, type, false, path);

            fields.add(field);
        }
        return new EntryMetadata(
                GenericQueryTargetDescriptor.DEFAULT,
                new PortableUpsertTargetDescriptor(
                        classDefinition.getFactoryId(),
                        classDefinition.getClassId(),
                        classDefinition.getVersion()
                ),
                fields
        );
    }

    private ClassDefinition resolveClassDefinition(
            boolean isKey,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        String factoryIdProperty = isKey ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID;
        String factoryId = options.get(factoryIdProperty);
        String classIdProperty = isKey ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID;
        String classId = options.get(classIdProperty);
        String classVersionProperty = isKey ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION;
        String classVersion = options.get(classVersionProperty);

        if (factoryId == null || classId == null || classVersion == null) {
            throw QueryException.error(
                    "Unable to resolve table metadata. Missing ['"
                            + factoryIdProperty + "'|'"
                            + classIdProperty + "'|'"
                            + classVersionProperty
                            + "'] option(s)");
        }

        return lookupClassDefinition(
                serializationService,
                Integer.parseInt(factoryId),
                Integer.parseInt(classId),
                Integer.parseInt(classVersion)
        );
    }
}
