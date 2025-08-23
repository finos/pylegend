package org.finos.legend.pylegend;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.finos.legend.engine.protocol.pure.v1.PureProtocolObjectMapperFactory;

import java.util.*;

public class ServerState
{
    public static class TableMetadata
    {
        public final List<Map<String, Object>> rows = new ArrayList<>();
        public final List<Column> schema;

        public TableMetadata(List<Column> schema)
        {
            this.schema = schema;
        }
    }

    public static class Column
    {
        public final String name;
        public final String type;
        public final boolean isPrimaryKey;

        public Column(String name, String type, boolean isPrimaryKey)
        {
            this.name = name;
            this.type = type;
            this.isPrimaryKey = isPrimaryKey;
        }
    }
    public static final Map<String, Map<String, Map<String, Map<String, TableMetadata>>>> data = new HashMap<>();
}




