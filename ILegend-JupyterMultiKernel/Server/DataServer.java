package org.finos.legend.pylegend;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Authenticator;
import org.eclipse.collections.api.RichIterable;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.language.pure.grammar.from.RelationalGrammarParserExtension;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposer;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposerContext;
import org.finos.legend.engine.plan.execution.api.result.ResultManager;
import org.finos.legend.engine.plan.execution.result.ConstantResult;
import org.finos.legend.engine.plan.execution.result.Result;
import org.finos.legend.engine.plan.execution.result.serialization.SerializationFormat;
import org.finos.legend.engine.plan.execution.stores.relational.result.RelationalResult;
import org.finos.legend.engine.plan.generation.PlanGenerator;
import org.finos.legend.engine.plan.generation.transformers.LegendPlanTransformers;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.SingleExecutionPlan;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.model.Database;
import org.finos.legend.engine.pure.code.core.PureCoreExtensionLoader;
import org.finos.legend.engine.shared.core.identity.Identity;
import org.finos.legend.engine.shared.core.kerberos.SubjectTools;
import org.finos.legend.engine.shared.core.operational.logs.LoggingEventType;
import org.finos.legend.pure.generated.Root_meta_pure_executionPlan_ExecutionPlan;
import org.finos.legend.pure.generated.Root_meta_pure_extension_Extension;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.finos.legend.engine.plan.execution.PlanExecutor;
import org.finos.legend.engine.plan.execution.stores.relational.config.RelationalExecutionConfiguration;
import org.finos.legend.engine.plan.execution.stores.relational.config.TemporaryTestDbConfiguration;
import org.finos.legend.engine.plan.execution.stores.relational.connection.authentication.strategy.OAuthProfile;
import org.finos.legend.engine.plan.execution.stores.relational.connection.manager.ConnectionManagerSelector;
import org.finos.legend.engine.plan.execution.stores.relational.plugin.Relational;
import org.finos.legend.engine.plan.execution.stores.relational.plugin.RelationalStoreExecutor;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.RelationalDatabaseConnection;
import java.util.Collections;
import org.finos.legend.engine.repl.client.jline3.JLine3Completer;
import org.finos.legend.engine.repl.client.jline3.JLine3Parser;
import org.finos.legend.engine.repl.core.legend.LegendInterface;
import org.finos.legend.engine.repl.core.legend.LocalLegendInterface;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import org.finos.legend.engine.repl.client.Client;
import java.sql.*;
import org.finos.legend.engine.repl.dataCube.DataCubeReplExtension;
import org.finos.legend.engine.repl.relational.RelationalReplExtension;
import org.finos.legend.engine.repl.relational.autocomplete.RelationalCompleterExtension;
import org.finos.legend.engine.repl.relational.shared.ConnectionHelper;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.jline.reader.LineReaderBuilder;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;




@Path("/server")
public class DataServer
{
    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports();
    private static final LegendInterface REPL_INTERFACE = new LocalLegendInterface();
    private static PlanExecutor PLAN_EXECUTOR;
    private static ConnectionManagerSelector CONNECTION_MANAGER;
    private static Client CLIENT;
    private static JLine3Parser PARSER ;
    private static JLine3Completer COMPLETER ;
    private static LineReader LINE_READER;
    private static final Map<String, String> MACROS = new HashMap<>();

    private static PlanExecutor buildPlanExecutor()
    {
        RelationalExecutionConfiguration relationalExecutionConfiguration = RelationalExecutionConfiguration.newInstance()
                .withTempPath(System.getProperty("TMP","/tmp/"))
                .withTemporaryTestDbConfiguration(new TemporaryTestDbConfiguration(9095))
                .build();
        RelationalStoreExecutor relationalStoreExecutor = (RelationalStoreExecutor) Relational.build(relationalExecutionConfiguration);
        CONNECTION_MANAGER = relationalStoreExecutor.getStoreState().getRelationalExecutor().getConnectionManager();
        return PlanExecutor.newPlanExecutorBuilder().withStoreExecutors(relationalStoreExecutor).build();

    }
    private static Result executeLine(String txt)
    {
        String expanded = expandMacros(txt);
        String code = "###Pure\n" +
                "function repl::__internal__::run():Any[*]\n{\n" + expanded + ";\n}";
        PureModelContextData d = CLIENT.getModelState().parseWithTransient(code);
        PureModel pureModel = REPL_INTERFACE.compile(d);
        RichIterable<? extends Root_meta_pure_extension_Extension> extensions = PureCoreExtensionLoader.extensions().flatCollect(e->e.extraPureCoreExtensions(pureModel.getExecutionSupport()));

        Root_meta_pure_executionPlan_ExecutionPlan plan = REPL_INTERFACE.generatePlan(pureModel,false);
        String planStr = PlanGenerator.serializeToJSON(plan,"vX_X_X",pureModel,extensions, LegendPlanTransformers.transformers);

        Result res = PLAN_EXECUTOR.execute((SingleExecutionPlan) PlanExecutor.readExecutionPlan(planStr),Maps.mutable.empty(),"user", Identity.getAnonymousIdentity());
        if(res instanceof RelationalResult || res instanceof ConstantResult)
        {
            return res;
        }
        throw new RuntimeException(res.getClass() + "result type not supported");
    }
    private static final Pattern MACRO_PATTERN = Pattern.compile("macro\\{(.*?)\\}");
    private static String expandMacros(String code)
    {
            Matcher matcher = MACRO_PATTERN.matcher(code);
            StringBuffer result = new StringBuffer();
            while (matcher.find())
            {
                String macroKey = matcher.group(1);
                String replacement = MACROS.getOrDefault(macroKey, "macro{" + macroKey + "}");
                matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
            }
            matcher.appendTail(result);
            return result.toString();
    }

    @POST
    @Path("/execute")
    public Response execute(@Context HttpServletRequest request, @Context HttpServletResponse response) throws Exception
    {
        RelationalReplExtension relationalReplExtension = new RelationalReplExtension();
        PLAN_EXECUTOR = buildPlanExecutor();
        CLIENT = new Client(Lists.mutable.with(relationalReplExtension,new DataCubeReplExtension()), Lists.mutable.with(new RelationalCompleterExtension()),PLAN_EXECUTOR);
//      CLIENT.commands.add(0,new LoadProjectCommand(CLIENT));
        PARSER = new JLine3Parser();
        COMPLETER = new JLine3Completer(CLIENT.commands);
        LINE_READER = (LineReader) LineReaderBuilder.builder().parser(PARSER).completer(COMPLETER).build();
        try
        {
            Map<String,Object> payload = OBJECT_MAPPER.readValue(request.getInputStream(), new TypeReference<Map<String, Object>>(){});
            String line = (String) payload.get("line");
            if (line.trim().startsWith("macro "))
            {
                Pattern macroPattern = Pattern.compile("macro\\s+(\\w+)\\s*=\\s*(.+);?");
                Matcher matcher = macroPattern.matcher(line);
                if (matcher.matches())
                {
                    String name = matcher.group(1);
                    String value = matcher.group(2).trim();
                    MACROS.put(name, value);
                    return Response.ok("Macro '" + name + "' defined.").build();
                }
                else
                {
                    return Response.status(400).entity("Invalid macro format. Use: macro name = value;").build();
                }
            }
            else if (line.trim().startsWith("show_macros"))
            {
                StringBuilder sb = new StringBuilder("Defined Macros:\n\n");
                for (Map.Entry<String, String> entry : MACROS.entrySet())
                {
                    sb.append("macro{").append(entry.getKey()).append("} = ").append(entry.getValue()).append("\n\n");
                }
                return Response.ok(sb.toString()).build();
            }
            else if (line.trim().startsWith("clear_macros"))
            {
                MACROS.clear();
                return Response.ok("All macros cleared.").build();
            }
            if(line.startsWith("load "))
            {
                String[] tokens = line.split(" ");
                if(tokens.length < 3 || tokens.length > 4)
                {
                   //throw new RuntimeException(String.format("Error, load should be used as '%s",CLIENT.commands.selectInstancesOf(Load.class).get(0).documentation));
                     throw new RuntimeException(String.format("Error in using the load command"));
                }
                RelationalDatabaseConnection databaseConnection = (RelationalDatabaseConnection) org.finos.legend.engine.repl.relational.shared.ConnectionHelper.getDatabaseConnection(CLIENT.getModelState().parse(),tokens[2]);
                try(Connection connection = org.finos.legend.engine.repl.relational.shared.ConnectionHelper.getConnection(databaseConnection,PLAN_EXECUTOR))
                {
                    String tableName = tokens.length == 4 ? tokens[3] : ("test"+(ConnectionHelper.getTables(databaseConnection,PLAN_EXECUTOR).count()+1));
                    int rowCount = 0;
                    try(Statement statement = connection.createStatement())
                    {
                        statement.executeUpdate(org.finos.legend.engine.plan.execution.stores.relational.connection.driver.DatabaseManager.fromString(databaseConnection.type.name()).relationalDatabaseSupport().load(tableName, tokens[1]));
                        try (ResultSet rs = statement.executeQuery("select count(*) as cnt from " + tableName)) {
                            rs.next();
                            rowCount = rs.getInt(1);
                        }
                    }
                    return Response.ok("Loaded into table: '" + tableName + "'. RowCount: "+rowCount).build();
                }
            }
            else if(line.startsWith("db"))
            {
                String[] tokens = line.split(" ");
                if(tokens.length!=2) throw new RuntimeException("db should be used as 'db <connection>'");
                RelationalDatabaseConnection databaseConnection = ConnectionHelper.getDatabaseConnection(CLIENT.getModelState().parse(),tokens[1]);
                Database database = ConnectionHelper.getDatabase(databaseConnection, "local","DB",PLAN_EXECUTOR);
                return Response.ok(PureGrammarComposer.newInstance(PureGrammarComposerContext.Builder.newInstance().build()).render(database, RelationalGrammarParserExtension.NAME)).build();
            }
            else if (line.startsWith("drop_all_tables "))
            {
                String[] tokens = line.split(" ");
                if (tokens.length != 2)
                {
                    throw new RuntimeException("drop_all_tables should be used as 'drop_all_tables <connection>'");
                }
                String connectionName = tokens[1];
                RelationalDatabaseConnection databaseConnection = ConnectionHelper.getDatabaseConnection(CLIENT.getModelState().parse(), connectionName);
                try (Connection connection = ConnectionHelper.getConnection(databaseConnection, PLAN_EXECUTOR))
                {
                    List<String> tableNames = ConnectionHelper.getTables(databaseConnection, PLAN_EXECUTOR)
                            .map(t -> t.name)
                            .collect(Collectors.toList());
                    try (Statement stmt = connection.createStatement())
                    {
                        for (String table : tableNames)
                        {
                            stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
                        }
                    }
                    return Response.ok("Dropped tables: " + String.join(", ", tableNames)).build();
                }
            }
            else if (line.startsWith("get_tables"))
            {
                String[] tokens = line.split(" ");
                if (tokens.length != 2)
                {
                    throw new RuntimeException("Usage: get_tables <connection>");
                }
                String connectionName = tokens[1];
                RelationalDatabaseConnection databaseConnection = ConnectionHelper.getDatabaseConnection(CLIENT.getModelState().parse(), connectionName);
                List<String> tableNames = ConnectionHelper.getTables(databaseConnection, PLAN_EXECUTOR)
                            .map(t -> t.name)
                            .collect(Collectors.toList());
                return Response.ok(Collections.singletonMap("tables", tableNames)).type(MediaType.APPLICATION_JSON).build();
            }
            else if (line.startsWith("get_attributes"))
            {
                String fullRef = line.substring("get_attributes".length()).trim();
                int dotIndex = fullRef.lastIndexOf('.');
                if (dotIndex == -1)
                {
                    throw new RuntimeException("Usage: get_attributes <connectionName>.<tableName>");
                }
                String connectionName = fullRef.substring(0, dotIndex);
                String tableName = fullRef.substring(dotIndex + 1);
                RelationalDatabaseConnection databaseConnection =
                        ConnectionHelper.getDatabaseConnection(CLIENT.getModelState().parse(), connectionName);
                List<?> tables = ConnectionHelper.getTables(databaseConnection, PLAN_EXECUTOR)
                        .collect(Collectors.toList());
                Object matchingTable = tables.stream()
                        .filter(t -> {
                            try {
                                java.lang.reflect.Field nameField = t.getClass().getField("name");
                                String name = (String) nameField.get(t);
                                return name.equalsIgnoreCase(tableName);
                            } catch (Exception e) {
                                return false;
                            }
                        })
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("Table '" + tableName + "' not found in connection '" + connectionName + "'"));
                List<String> attributeNames = new ArrayList<>();
                try {
                    java.lang.reflect.Field columnsField = matchingTable.getClass().getField("columns");
                    List<?> columns = (List<?>) columnsField.get(matchingTable);

                    for (Object col : columns)
                    {
                        java.lang.reflect.Field colNameField = col.getClass().getField("name");
                        String colName = (String) colNameField.get(col);
                        attributeNames.add(colName);
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Error extracting column names: " + e.getMessage(), e);
                }
                return Response.ok(Collections.singletonMap("attributes", attributeNames))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }

            else if (line.startsWith("get_all"))
            {
                PureModelContextData modelContextData = CLIENT.getModelState().parse();
                List<String> connectionNames = modelContextData.getElements().stream()
                        .map(e -> e.name)
                        .collect(Collectors.toList());
                return Response.ok(Collections.singletonMap("connections", connectionNames)).type(MediaType.APPLICATION_JSON).build();
            }
            else if(line.startsWith("complete"))
            {
                List<Candidate> candidates = Lists.mutable.empty();
                String[] words = line.trim().split("\\s+");
                String line_new = words.length >= 2 ? words[1] : "";
                COMPLETER.complete(LINE_READER,PARSER.parse(line_new,line_new.length(),null),candidates);
                return Response.ok(Collections.singletonMap("completions",candidates.stream().map(Candidate::displ).distinct().collect(Collectors.toList()))).type(MediaType.APPLICATION_JSON).build();
            }
            else
            {
                CLIENT.addCommandToHistory(line);
                Result res = executeLine(line);
                return ResultManager.manageResult("unknown",res, SerializationFormat.RAW, LoggingEventType.EXECUTE_INTERACTIVE_STOP);
            }
        }
        catch (Exception e)
        {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(Collections.singletonMap("error", e.getMessage())).type(MediaType.APPLICATION_JSON).build();
        }
    }
}
