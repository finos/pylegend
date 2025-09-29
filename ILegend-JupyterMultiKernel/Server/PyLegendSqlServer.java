// Copyright 2023 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.pylegend;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.hazelcast.shaded.org.snakeyaml.engine.v2.api.Load;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.finos.legend.authentication.credentialprovider.CredentialProviderProvider;
import org.finos.legend.engine.authentication.LegendDefaultDatabaseAuthenticationFlowProviderConfiguration;
import org.finos.legend.engine.functionActivator.api.FunctionActivatorAPI;
import org.finos.legend.engine.functionActivator.service.FunctionActivatorService;
import org.finos.legend.engine.language.hostedService.api.HostedServiceService;
import org.finos.legend.engine.language.pure.compiler.Compiler;
import org.finos.legend.engine.language.pure.compiler.api.Compile;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.language.pure.grammar.api.grammarToJson.GrammarToJson;
import org.finos.legend.engine.language.pure.grammar.api.jsonToGrammar.JsonToGrammar;
import org.finos.legend.engine.language.pure.modelManager.ModelManager;
import org.finos.legend.engine.language.pure.modelManager.sdlc.SDLCLoader;
import org.finos.legend.engine.language.snowflakeApp.api.SnowflakeAppService;
import org.finos.legend.engine.plan.execution.PlanExecutor;
import org.finos.legend.engine.plan.execution.api.concurrent.ConcurrentExecutionNodeExecutorPoolInfo;
import org.finos.legend.engine.plan.execution.stores.relational.api.RelationalExecutorInformation;
import org.finos.legend.engine.plan.execution.stores.relational.config.RelationalExecutionConfiguration;
import org.finos.legend.engine.plan.execution.stores.relational.config.TemporaryTestDbConfiguration;
import org.finos.legend.engine.plan.execution.stores.relational.plugin.Relational;
import org.finos.legend.engine.plan.execution.stores.relational.plugin.RelationalStoreExecutor;
import org.finos.legend.engine.plan.generation.extension.PlanGeneratorExtension;
import org.finos.legend.engine.protocol.pure.v1.PureProtocolObjectMapperFactory;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.pure.code.core.PureCoreExtensionLoader;
import org.finos.legend.engine.query.sql.api.SQLExecutor;
import org.finos.legend.engine.query.sql.api.execute.SqlExecute;
import org.finos.legend.engine.query.sql.api.grammar.SqlGrammar;
import org.finos.legend.engine.query.sql.providers.LegendServiceSQLSourceProvider;
import org.finos.legend.engine.query.sql.providers.RelationalStoreSQLSourceProvider;
import org.finos.legend.engine.query.sql.providers.shared.FunctionSQLSourceProvider;
import org.finos.legend.engine.query.sql.providers.shared.project.ProjectCoordinateLoader;
import org.finos.legend.engine.server.Server;
import org.finos.legend.engine.server.ServerConfiguration;
import org.finos.legend.engine.server.core.api.CurrentUser;
import org.finos.legend.engine.server.core.api.Info;
import org.finos.legend.engine.server.core.api.Memory;
import org.finos.legend.engine.server.core.exceptionMappers.CatchAllExceptionMapper;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.finos.legend.engine.shared.core.deployment.DeploymentMode;
import org.finos.legend.engine.shared.core.deployment.DeploymentStateAndVersions;
import org.finos.legend.pure.generated.Root_meta_pure_extension_Extension;
import org.finos.legend.server.pac4j.LegendPac4jBundle;



import java.util.Collections;
import java.util.ServiceLoader;

public class PyLegendSqlServer<T extends ServerConfiguration> extends Server<T>
{
    public static void main(String[] args) throws Exception {
        new PyLegendSqlServer<>().run(args);
    }

    @Override
    public void initialize(Bootstrap<T> bootstrap)
    {
        bootstrap.addBundle(new LegendPac4jBundle<>(serverConfiguration -> serverConfiguration.pac4j));
        bootstrap.setConfigurationSourceProvider(new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor(true)));
//        bootstrap.addBundle(new SwaggerBundle<T>() {
//            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(T configuration) {
//                return configuration.swagger;
//            }
//        });
        PureProtocolObjectMapperFactory.withPureProtocolExtensions(bootstrap.getObjectMapper());
        ObjectMapperFactory.withStandardConfigurations(bootstrap.getObjectMapper());
        bootstrap.getObjectMapper().registerSubtypes(new NamedType(LegendDefaultDatabaseAuthenticationFlowProviderConfiguration.class, "legendDefault"));
    }

    @Override
    public void run(T serverConfiguration, Environment environment)
    {
        DeploymentStateAndVersions.DEPLOYMENT_MODE = DeploymentMode.TEST_IGNORE_FUNCTION_MATCH;

        SDLCLoader sdlcLoader = new SDLCLoader(serverConfiguration.metadataserver, null);
        ModelManager modelManager = new ModelManager(DeploymentMode.TEST_IGNORE_FUNCTION_MATCH, sdlcLoader);

        RelationalExecutionConfiguration relationalExecution = new RelationalExecutionConfiguration();
        TemporaryTestDbConfiguration temporaryTestDbConfiguration = new TemporaryTestDbConfiguration();
        temporaryTestDbConfiguration.port = 9092;
        relationalExecution.tempPath = "/tmp";
        relationalExecution.temporarytestdb = temporaryTestDbConfiguration;
        relationalExecution.setCredentialProviderProvider(CredentialProviderProvider.defaultProviderProvider());
        relationalStoreExecutor = (RelationalStoreExecutor) Relational.build(relationalExecution);

        PlanExecutor planExecutor = PlanExecutor.newPlanExecutor(relationalStoreExecutor);

        // API
        environment.jersey().setUrlPattern("/api/*");

        // Server
        environment.jersey().register(new Info(serverConfiguration.deployment, serverConfiguration.opentracing));
        environment.jersey().register(new CurrentUser());
        environment.jersey().register(new Memory());
        environment.jersey().register(new RelationalExecutorInformation());
        environment.jersey().register(new ConcurrentExecutionNodeExecutorPoolInfo(Collections.emptyList()));

        MutableList<PlanGeneratorExtension> generatorExtensions = Lists.mutable.withAll(ServiceLoader.load(PlanGeneratorExtension.class));
        Function<PureModel, RichIterable<? extends Root_meta_pure_extension_Extension>> routerExtensions = (pureModel) -> {
            return PureCoreExtensionLoader.extensions().flatCollect((e) -> {
                return e.extraPureCoreExtensions(pureModel.getExecutionSupport());
            });
        };
        ProjectCoordinateLoader projectCoordinateLoader = new ProjectCoordinateLoader(modelManager, serverConfiguration.metadataserver.getSdlc());
        environment.jersey().register(new SqlExecute(new SQLExecutor(modelManager, planExecutor, routerExtensions, FastList.newListWith(
                new RelationalStoreSQLSourceProvider(projectCoordinateLoader),
                new FunctionSQLSourceProvider(projectCoordinateLoader),
                new LegendServiceSQLSourceProvider(projectCoordinateLoader)),
                generatorExtensions.flatCollect(PlanGeneratorExtension::getExtraPlanTransformers))));
        environment.jersey().register(new SqlGrammar());
        environment.jersey().register(new CatchAllExceptionMapper());
        environment.jersey().register(new Compile(modelManager));

        environment.jersey().register(new GrammarToJson());
        environment.jersey().register(new JsonToGrammar());
        environment.jersey().register(new LoadController());
        environment.jersey().register(new DataServer());
        environment.jersey().register(new FunctionActivatorAPI(modelManager, Lists.mutable.empty(), Lists.mutable.with(new FunctionActivatorService[]{new SnowflakeAppService(planExecutor), new HostedServiceService()}), routerExtensions));
        //Compiler.compile(PureModelContextData.newBuilder().build(), DeploymentMode.TEST_IGNORE_FUNCTION_MATCH, Lists.mutable.empty());
    }
}
