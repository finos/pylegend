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

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.utility.LazyIterate;
import org.finos.legend.engine.language.pure.modelManager.ModelManager;
import org.finos.legend.engine.language.pure.modelManager.sdlc.SDLCLoader;
import org.finos.legend.engine.language.pure.modelManager.sdlc.configuration.MetaDataServerConfiguration;
import org.finos.legend.engine.language.pure.modelManager.sdlc.configuration.ServerConnectionConfiguration;
import org.finos.legend.engine.protocol.pure.PureClientVersions;
import org.finos.legend.engine.protocol.pure.v1.model.context.AlloySDLC;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextPointer;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.service.PureSingleExecution;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.service.Service;
import org.finos.legend.engine.query.sql.api.sources.*;
import org.finos.legend.engine.shared.core.deployment.DeploymentMode;
import org.pac4j.core.profile.CommonProfile;

import java.util.List;

public class LegendServiceSQLSourceProvider implements SQLSourceProvider
{
    private static final String LEGEND_SERVICE = "legend_service";

    @Override
    public String getType()
    {
        return LEGEND_SERVICE;
    }

    @Override
    public SQLSourceResolvedContext resolve(List<TableSource> sources, SQLContext context, MutableList<CommonProfile> profiles)
    {
        MetaDataServerConfiguration metaDataServerConfiguration = new MetaDataServerConfiguration(null, new ServerConnectionConfiguration("localhost", Integer.parseInt(System.getProperty("dw.metadataserver.alloy.port"))));
        ModelManager modelManager = new ModelManager(DeploymentMode.TEST_IGNORE_FUNCTION_MATCH, new SDLCLoader(metaDataServerConfiguration, null));
        List<SQLSource> sqlSources = FastList.newList();
        if (sources.size() != 1) throw new RuntimeException("Multiple sources not supported");

        TableSource source = sources.get(0);

        String pattern = (String) source.getArgument("pattern", 0).getValue();

        PureModelContextPointer pointer = new PureModelContextPointer();
        AlloySDLC alloySDLC = new AlloySDLC();
        alloySDLC.groupId = source.getNamedArgument("groupId").orElseThrow().getValue().toString();
        alloySDLC.artifactId = source.getNamedArgument("artifactId").orElseThrow().getValue().toString();
        alloySDLC.version = source.getNamedArgument("version").orElseThrow().getValue().toString();
        pointer.sdlcInfo = alloySDLC;
        PureModelContextData pureModelContextData = modelManager.loadData(pointer, PureClientVersions.production, Lists.mutable.empty());

        Service service = LazyIterate.select(pureModelContextData.getElements(), e -> e instanceof Service)
                .collect(e -> (Service) e)
                .select(s -> s.pattern.equals(pattern))
                .getFirst();

        List<SQLSourceArgument> keys = FastList.newListWith(new SQLSourceArgument("pattern", 0, pattern));
        keys.add(new SQLSourceArgument("groupId", 1, alloySDLC.groupId));
        keys.add(new SQLSourceArgument("artifactId", 2, alloySDLC.artifactId));
        keys.add(new SQLSourceArgument("version", 3, alloySDLC.version));

        if (service.execution instanceof PureSingleExecution)
        {
            sqlSources.add(from((PureSingleExecution) service.execution, keys));
        }
        return new SQLSourceResolvedContext(pureModelContextData, sqlSources);
    }

    private SQLSource from(PureSingleExecution pse, List<SQLSourceArgument> keys)
    {
        return new SQLSource(LEGEND_SERVICE, pse.func, pse.mapping, pse.runtime, pse.executionOptions, keys);
    }
}
