/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.extension.AbstractExtensionLoader;
import co.cask.cdap.proto.ProgramType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A singleton class for discovering {@link ProgramRuntimeProvider} through the runtime extension mechanism that uses
 * the Java {@link ServiceLoader} architecture.
 */
@Singleton
public class ProgramRuntimeProviderLoader extends AbstractExtensionLoader<ProgramType, ProgramRuntimeProvider> {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramRuntimeProviderLoader.class);

  // The ProgramRunnerProvider serves as a tagging instance to indicate there is not
  // provider supported for a given program type
  private static final ProgramRuntimeProvider NOT_SUPPORTED_PROVIDER = new ProgramRuntimeProvider() {
    @Override
    public ProgramRunner createProgramRunner(ProgramType type, Mode mode, Injector injector) {
      throw new UnsupportedOperationException();
    }
  };


  @VisibleForTesting
  @Inject
  public ProgramRuntimeProviderLoader(CConfiguration cConf) {
    super(cConf.get(Constants.AppFabric.RUNTIME_EXT_DIR, ""), ProgramRuntimeProvider.class, NOT_SUPPORTED_PROVIDER);
  }

  /**
   * Returns a {@link ProgramRuntimeProvider} if one is found for the given {@link ProgramType};
   * otherwise {@code null} will be returned.
   */
  @Nullable
  public ProgramRuntimeProvider get(ProgramType programType) {
    try {
      ProgramRuntimeProvider provider = loadExtension(programType);
      if (provider != NOT_SUPPORTED_PROVIDER) {
        return provider;
      }
    } catch (Throwable t) {
      LOG.warn("Failed to load ProgramRunnerProvider for {} program.", programType, t);
    }
    return null;
  }

  @Override
  public Set<ProgramType> getSupportedTypesForProvider(ProgramRuntimeProvider programRuntimeProvider) {
    // See if the provide supports the required program type
    ProgramRuntimeProvider.SupportedProgramType supportedTypes =
      programRuntimeProvider.getClass().getAnnotation(ProgramRuntimeProvider.SupportedProgramType.class);
    return supportedTypes == null ? ImmutableSet.<ProgramType>of() : ImmutableSet.copyOf(supportedTypes.value());
  }
}
