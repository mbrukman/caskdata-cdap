/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.metrics.query;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.ServerException;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.metrics.data.AggregatesScanResult;
import co.cask.cdap.metrics.data.AggregatesScanner;
import co.cask.cdap.metrics.data.AggregatesTable;
import co.cask.cdap.metrics.data.MetricsTableFactory;
import co.cask.http.HandlerContext;
import co.cask.http.HttpResponder;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Class for handling requests for aggregate application metrics.
 */
@Path(Constants.Gateway.API_VERSION_2 + "/metrics/available")
//todo : clean up the /apps/ endpoints after deprecating old-UI (CDAP-1111)
public final class MetricsDiscoveryHandler extends BaseMetricsHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsDiscoveryHandler.class);

  private final Supplier<AggregatesTable> aggregatesTable;

  // known 'program types' in a metric context (app.programType.programId.componentId)
  private enum ProgramType {
    PROCEDURES("p"),
    MAPREDUCE("b"),
    FLOWS("f"),
    STREAMS("stream"),
    DATASETS("dataset"),
    UNKNOWN("");
    String id;

    private ProgramType(String id) {
      this.id = id;
    }

    private static ProgramType fromId(String id) {
      for (ProgramType p : ProgramType.values()) {
        if (p.id.equals(id)) {
          return p;
        }
      }
      LOG.debug("unknown program type: " + id);
      return UNKNOWN;
    }
  }

  // used to display the type of a context node in the response
  private enum ContextNodeType {
    APP("app"),
    FLOW("flow"),
    FLOWLET("flowlet"),
    PROCEDURE("procedure"),
    MAPREDUCE("mapreduce"),
    MAPREDUCE_TASK("mapreduceTask"),
    STREAM("stream"),
    DATASET("dataset"),
    ROOT("root");
    String name;

    private ContextNodeType(String name) {
      this.name = name;
    }

    private String getName() {
      return name;
    }
  }

  // the context has 'm' and 'r', but they're referred to as 'mappers' and 'reducers' in the api.
  private enum MapReduceTask {
    M("mappers"),
    R("reducers");

    private final String name;

    private MapReduceTask(String name) {
      this.name = name;
    }

    private String getName() {
      return name;
    }
  }

  @Inject
  public MetricsDiscoveryHandler(Authenticator authenticator, final MetricsTableFactory metricsTableFactory) {
    super(authenticator);

    this.aggregatesTable = Suppliers.memoize(new Supplier<AggregatesTable>() {
      @Override
      public AggregatesTable get() {
        return metricsTableFactory.createAggregates();
      }
    });
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    LOG.info("Starting MetricsDiscoveryHandler");
  }

  @Override
  public void destroy(HandlerContext context) {
    super.destroy(context);
    LOG.info("Stopping MetricsDiscoveryHandler");
  }

  @GET
  public void handleOverview(HttpRequest request, HttpResponder responder,
                             @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    getMetrics(request, responder, metricPrefix);
  }

  // ex: /apps/appX
  @GET
  @Path("/apps/{app-id}")
  public void handleApp(HttpRequest request, HttpResponder responder,
                        @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    getMetrics(request, responder, metricPrefix);
  }

  // ex: /apps/appX/flows
  @GET
  @Path("/apps/{app-id}/{program-type}")
  public void handleProgramType(HttpRequest request, HttpResponder responder,
                                @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    getMetrics(request, responder, metricPrefix);
  }

  // ex: /apps/appX/flows/flowY
  @GET
  @Path("/apps/{app-id}/{program-type}/{program-id}")
  public void handleProgram(HttpRequest request, HttpResponder responder,
                            @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    getMetrics(request, responder, metricPrefix);
  }

  // ex: /apps/appX/mapreduce/mapredY/mappers
  @GET
  @Path("/apps/{app-id}/{program-type}/{program-id}/{component-type}")
  public void handleComponentType(HttpRequest request, HttpResponder responder,
                                  @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    getMetrics(request, responder, metricPrefix);
  }

  // ex: /apps/appX/flows/flowY/flowlets/flowletZ
  @GET
  @Path("/apps/{app-id}/{program-type}/{program-id}/{component-type}/{component-id}")
  public void handleComponent(HttpRequest request, HttpResponder responder,
                              @QueryParam("prefixEntity") String metricPrefix) throws IOException {
    getMetrics(request, responder, metricPrefix);
  }

  private void getMetrics(HttpRequest request, HttpResponder responder, String metricPrefix) {
    String contextPrefix = null;
    try {
      String path = request.getUri();
      String base = Constants.Gateway.API_VERSION_2 + "/metrics/available/apps";
      if (path.startsWith(base)) {
        Iterator<String> pathParts = Splitter.on('/').split(path.substring(base.length() + 1)).iterator();
        MetricsRequestContext.Builder builder = new MetricsRequestContext.Builder();
        builder.setNamespaceId(Constants.DEFAULT_NAMESPACE);
        MetricsRequestParser.parseSubContext(pathParts, builder);
        MetricsRequestContext metricsRequestContext = builder.build();
        contextPrefix = metricsRequestContext.getContextPrefix();
        validatePathElements(request, metricsRequestContext);
      }
    } catch (MetricsPathException e) {
      responder.sendError(HttpResponseStatus.NOT_FOUND, e.getMessage());
      return;
    } catch (ServerException e) {
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while looking for metrics");
      return;
    }

    Map<String, ContextNode> metricContextsMap = Maps.newHashMap();
    AggregatesTable table = aggregatesTable.get();
    AggregatesScanner scanner = table.scanRowsOnly(contextPrefix, metricPrefix);

    // scanning through all metric rows in the aggregates table
    // row has context plus metric info
    // each metric can show up in multiple contexts
    while (scanner.hasNext()) {
      AggregatesScanResult result = scanner.next();
      addContext(result.getContext(), result.getMetric(), metricContextsMap);
    }

    // return the metrics sorted by metric name so it can directly be displayed to the user.
    JsonArray output = new JsonArray();
    List<String> sortedMetrics = Lists.newArrayList(metricContextsMap.keySet());
    Collections.sort(sortedMetrics);
    for (String metric : sortedMetrics) {
      JsonObject metricNode = new JsonObject();
      metricNode.addProperty("metric", metric);
      ContextNode metricContexts = metricContextsMap.get(metric);
      // the root node has junk for its type and id, but has the list of contexts as its "children"
      metricNode.add("contexts", metricContexts.toJson().getAsJsonArray("children"));
      output.add(metricNode);
    }

    responder.sendJson(HttpResponseStatus.OK, output);
  }

  /**
   * Eventually we need to return the tree of contexts each metric belongs to, but we're not going to get
   * all the contexts for a metric in order.  This is to add a node to the context tree, where the tree
   * will end up looking something like:
   * <p/>
   * "contexts":[
   * {
   * "type":"app",
   * "id":"WordCount",
   * "children":[
   * {
   * "type":"flow",
   * "id":"WordCounter",
   * "children":[
   * {
   * "type":"flowlet",
   * "id":"Associator"
   * },...
   * ]
   * },...
   * ]
   * },...
   * ]
   */
  private void addContext(String context, String metric, Map<String, ContextNode> metricContextsMap) {
    Iterator<String> contextParts = Splitter.on('.').split(context).iterator();
    if (!contextParts.hasNext()) {
      return;
    }
    // get namespaceId as the first part, but ignore it since we do not need namespace in v2 APIs.
    // Adding to output when its not needed may impact UI so ignoring it.
    contextParts.next();
    if (!contextParts.hasNext()) {
      return;
    }
    String appId = contextParts.next();
    if (!contextParts.hasNext()) {
      return;
    }

    // get the context tree for the metric
    if (!metricContextsMap.containsKey(metric)) {
      metricContextsMap.put(metric, new ContextNode(ContextNodeType.ROOT, ""));
    }
    ContextNode metricContexts = metricContextsMap.get(metric);
    metricContexts = metricContexts.getOrAddChild(ContextNodeType.APP, appId);

    // different program types will have different depths in the context tree.  For example,
    // procedures have no children, whereas flows will have flowlets as children.
    ProgramType type = ProgramType.fromId(contextParts.next());
    switch (type) {
      case FLOWS:
        metricContexts.deepAdd(contextParts, ContextNodeType.FLOW, ContextNodeType.FLOWLET);
        break;
      case PROCEDURES:
        metricContexts.deepAdd(contextParts, ContextNodeType.PROCEDURE);
        break;
      case MAPREDUCE:
        if (contextParts.hasNext()) {
          metricContexts = metricContexts.getOrAddChild(ContextNodeType.MAPREDUCE, contextParts.next());
          if (contextParts.hasNext()) {
            String taskStr = MapReduceTask.valueOf(contextParts.next().toUpperCase()).getName();
            metricContexts.addChild(new ContextNode(ContextNodeType.MAPREDUCE_TASK, taskStr));
          }
        }
        break;
      // dataset and stream are special cases, currently the only "context" for them is "-.dataset" and "-.stream"
      case DATASETS:
        metricContexts.addChild(new ContextNode(ContextNodeType.DATASET, ""));
        break;
      case STREAMS:
        metricContexts.addChild(new ContextNode(ContextNodeType.STREAM, ""));
        break;
      default:
        break;
    }
  }

  // Class for building up the context tree for a metric.
  private class ContextNode implements Comparable<ContextNode> {
    private final ContextNodeType type;
    private final String id;
    private final Map<String, ContextNode> children;

    public ContextNode(ContextNodeType type, String id) {
      this.type = type;
      this.id = id;
      this.children = Maps.newHashMap();
    }

    // get the specified child, creating and adding it if it doesn't already exist
    public ContextNode getOrAddChild(ContextNodeType type, String id) {
      String key = getChildKey(type, id);
      if (!children.containsKey(key)) {
        children.put(key, new ContextNode(type, id));
      }
      return children.get(key);
    }

    // recursively add children of children of the given ids and types
    public void deepAdd(Iterator<String> ids, ContextNodeType... types) {
      ContextNode node = this;
      for (ContextNodeType type : types) {
        if (!ids.hasNext()) {
          break;
        }
        node = node.getOrAddChild(type, ids.next());
      }
    }

    public void addChild(ContextNode child) {
      children.put(getChildKey(child.getType(), child.getId()), child);
    }

    public ContextNodeType getType() {
      return type;
    }

    public String getId() {
      return id;
    }

    public String getKey() {
      return type.name() + ":" + id;
    }

    private String getChildKey(ContextNodeType type, String id) {
      return type.name() + ":" + id;
    }

    // convert to json to send back in a response.  Each node has a type and id, with
    // an optional array of children.
    public JsonObject toJson() {
      JsonObject output = new JsonObject();
      output.addProperty("type", type.getName());
      output.addProperty("id", id);

      if (children.size() > 0) {
        // maybe make the sort optional based on query params
        List<ContextNode> childList = Lists.newArrayList(children.values());
        Collections.sort(childList);
        JsonArray childrenJson = new JsonArray();
        for (ContextNode child : childList) {
          childrenJson.add(child.toJson());
        }
        output.add("children", childrenJson);
      }
      return output;
    }

    public int compareTo(ContextNode other) {
      return getKey().compareTo(other.getKey());
    }
  }

}
