package com.continuuity.app;

import com.continuuity.api.AbstractApplication;
import com.continuuity.api.ApplicationConfigurer;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.DatasetInstanceCreationSpec;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.internal.app.DefaultApplicationSpecification;
import com.continuuity.internal.batch.DefaultMapReduceSpecification;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.continuuity.internal.flow.DefaultFlowSpecification;
import com.continuuity.internal.procedure.DefaultProcedureSpecification;
import com.continuuity.internal.workflow.DefaultWorkflowSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

import java.util.Map;

/**
 * Default implementation of {@link ApplicationConfigurer}
 */
public class DefaultAppConfigurer implements ApplicationConfigurer {
  private String name;
  private String description;
  private final Map<String, StreamSpecification> streams = Maps.newHashMap();
  // TODO: to be removed after datasets API v1 is abandoned
  private final Map<String, DataSetSpecification> dataSets = Maps.newHashMap();
  private final Map<String, String> datasetModules = Maps.newHashMap();
  private final Map<String, DatasetInstanceCreationSpec> datasetInstances = Maps.newHashMap();
  private final Map<String, FlowSpecification> flows = Maps.newHashMap();
  private final Map<String, ProcedureSpecification> procedures = Maps.newHashMap();
  private final Map<String, MapReduceSpecification> mapReduces = Maps.newHashMap();
  private final Map<String, WorkflowSpecification> workflows = Maps.newHashMap();
  private final Map<String, TwillSpecification> services = Maps.newHashMap();
  // passed app to be used to resolve default name and description
  public DefaultAppConfigurer(AbstractApplication app) {
    this.name = app.getClass().getSimpleName();
    this.description = "";
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void addStream(Stream stream) {
    Preconditions.checkArgument(stream != null, "Stream cannot be null.");
    StreamSpecification spec = stream.configure();
    streams.put(spec.getName(), spec);
  }

  @Override
  public void addDataSet(DataSet dataSet) {
    Preconditions.checkArgument(dataSet != null, "DataSet cannot be null.");
    DataSetSpecification spec = dataSet.configure();
    dataSets.put(spec.getName(), spec);
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    Preconditions.checkArgument(moduleName != null, "Dataset module name cannot be null.");
    Preconditions.checkArgument(moduleClass != null, "Dataset module class cannot be null.");
    datasetModules.put(moduleName, moduleClass.getName());
  }

  @Override
  public void addDataSet(String datasetInstanceName, String typeName, DatasetInstanceProperties properties) {
    Preconditions.checkArgument(datasetInstanceName != null, "Dataset instance name cannot be null.");
    Preconditions.checkArgument(typeName != null, "Dataset type name cannot be null.");
    Preconditions.checkArgument(properties != null, "Instance properties name cannot be null.");
    datasetInstances.put(datasetInstanceName,
                         new DatasetInstanceCreationSpec(datasetInstanceName, typeName, properties));
  }

  @Override
  public void addFlow(Flow flow) {
    Preconditions.checkArgument(flow != null, "Flow cannot be null.");
    FlowSpecification spec = new DefaultFlowSpecification(flow.getClass().getName(), flow.configure());
    flows.put(spec.getName(), spec);
  }

  @Override
  public void addProcedure(Procedure procedure) {
    Preconditions.checkArgument(procedure != null, "Procedure cannot be null.");
    ProcedureSpecification spec = new DefaultProcedureSpecification(procedure, 1);
    procedures.put(spec.getName(), spec);
  }

  @Override
  public void addProcedure(Procedure procedure, int instance) {
    Preconditions.checkArgument(procedure != null, "Procedure cannot be null.");
    Preconditions.checkArgument(instance >= 1, "Number of instances can't be less than 1");
    ProcedureSpecification spec = new DefaultProcedureSpecification(procedure, instance);
    procedures.put(spec.getName(), spec);
  }

  @Override
  public void addMapReduce(MapReduce mapReduce) {
    Preconditions.checkArgument(mapReduce != null, "MapReduce cannot be null.");
    MapReduceSpecification spec = new DefaultMapReduceSpecification(mapReduce);
    mapReduces.put(spec.getName(), spec);
  }

  @Override
  public void addWorkflow(Workflow workflow) {
    Preconditions.checkArgument(workflow != null, "Workflow cannot be null.");
    WorkflowSpecification spec = new DefaultWorkflowSpecification(workflow.getClass().getName(),
                                                                  workflow.configure());
    workflows.put(spec.getName(), spec);

    // Add MapReduces from workflow into application
    mapReduces.putAll(spec.getMapReduce());
  }

  @Override
  public void addService(TwillApplication application) {
    Preconditions.checkNotNull(application, "Service cannot be null.");

    TwillSpecification specification = application.configure();
    services.put(specification.getName(), specification);

  }

  public ApplicationSpecification createApplicationSpec() {
    return new DefaultApplicationSpecification(name, description, streams, dataSets,
                                               datasetModules, datasetInstances,
                                               flows, procedures, mapReduces, workflows, services);
  }
}
