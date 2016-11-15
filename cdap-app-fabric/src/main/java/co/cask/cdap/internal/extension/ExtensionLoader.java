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

package co.cask.cdap.internal.extension;

import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.ImmutablePair;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A class to maintain a cache of extensions available in a configured extensions directory.
 *
 * @param <CACHE_KEY> the key type stored in the cache
 * @param <CACHE_VALUE> the value type stored in the cache
 */
public class ExtensionLoader<CACHE_KEY, CACHE_VALUE> {
  private static final Logger LOG = LoggerFactory.getLogger(ExtensionLoader.class);

  private final Class<CACHE_VALUE> cacheValueClass;
  // The ServiceLoader that loads extension implementation from the CDAP system classloader.
  private final ServiceLoader<CACHE_VALUE> systemExtensionLoader;
  private final Predicate<ImmutablePair<CACHE_KEY, CACHE_VALUE>> extensionFilter;
  private final LoadingCache<CACHE_KEY, CACHE_VALUE> extensionsCache;
  private final CACHE_VALUE defaultExtension;

  @SuppressWarnings("unchecked")
  public ExtensionLoader(String extDirs, Predicate<ImmutablePair<CACHE_KEY, CACHE_VALUE>> extensionFilter,
                         Class<CACHE_VALUE> cacheValueClass, CACHE_VALUE defaultExtension) {
    this.cacheValueClass = cacheValueClass;
    this.systemExtensionLoader = ServiceLoader.load(cacheValueClass);
    this.extensionFilter = extensionFilter;
    this.extensionsCache = createExtensionsCache(extDirs);
    this.defaultExtension = defaultExtension;
  }

  public CACHE_VALUE getExtension(CACHE_KEY key) {
    return extensionsCache.getUnchecked(key);
  }

  public Set<CACHE_KEY> listExtensions() {
    return extensionsCache.asMap().keySet();
  }

  private LoadingCache<CACHE_KEY, CACHE_VALUE> createExtensionsCache(String extDirs) {
    // A LoadingCache from extension directory to ServiceLoader
    final LoadingCache<File, ServiceLoader<CACHE_VALUE>> serviceLoaderCache = createServiceLoaderCache();

    final List<String> dirs = ImmutableList.copyOf(Splitter.on(';').omitEmptyStrings().trimResults().split(extDirs));

    return CacheBuilder.newBuilder().build(new CacheLoader<CACHE_KEY, CACHE_VALUE>() {
      @Override
      public CACHE_VALUE load(CACHE_KEY key) throws Exception {
        // Goes through all extension directory and see which service loader supports the give program type
        for (String dir : dirs) {
          File extDir = new File(dir);
          if (!extDir.isDirectory()) {
            continue;
          }

          // Each module would be under a directory of the extension directory
          for (File moduleDir : DirUtils.listFiles(extDir)) {
            if (!moduleDir.isDirectory()) {
              continue;
            }
            // Try to find a provider that can support the given program type.
            try {
              CACHE_VALUE extension = findExtension(serviceLoaderCache.getUnchecked(moduleDir), key);
              if (extension != null) {
                return extension;
              }
            } catch (Exception e) {
              LOG.warn("Exception raised when loading a ProgramRuntimeProvider from {}. Extension ignored.",
                       moduleDir, e);
            }
          }
        }

        // If there is none found in the ext dir, try to look it up from the CDAP system class ClassLoader.
        // This is for the unit-test case, which extensions are part of the test dependency, hence in the
        // unit-test ClassLoader.
        // If no provider was found, returns the NOT_SUPPORTED_PROVIDER so that we won't search again for
        // this program type.
        // Cannot use null because LoadingCache doesn't allow null value
        return Objects.firstNonNull(findExtension(systemExtensionLoader, key), defaultExtension);
      }
    });
  }

  /**
   * Creates a cache for caching extension directory to {@link ServiceLoader} of {@link CACHE_VALUE}.
   */
  private LoadingCache<File, ServiceLoader<CACHE_VALUE>> createServiceLoaderCache() {
    return CacheBuilder.newBuilder().build(new CacheLoader<File, ServiceLoader<CACHE_VALUE>>() {
      @Override
      public ServiceLoader<CACHE_VALUE> load(File dir) throws Exception {
        return createServiceLoader(dir);
      }
    });
  }

  /**
   * Creates a {@link ServiceLoader} from the {@link ClassLoader} created by all jar files under the given directory.
   */
  private ServiceLoader<CACHE_VALUE> createServiceLoader(File dir) {
    List<File> files = new ArrayList<>(DirUtils.listFiles(dir, "jar"));
    Collections.sort(files);

    URL[] urls = Iterables.toArray(Iterables.transform(files, new Function<File, URL>() {
      @Override
      public URL apply(File input) {
        try {
          return input.toURI().toURL();
        } catch (MalformedURLException e) {
          // Shouldn't happen
          throw Throwables.propagate(e);
        }
      }
    }), URL.class);

    URLClassLoader classLoader = new URLClassLoader(urls, ExtensionLoader.class.getClassLoader());
    return ServiceLoader.load(cacheValueClass, classLoader);
  }

  /**
   * Finds the first extension from the given {@link ServiceLoader} that the specified filter applies to.
   */
  @Nullable
  private CACHE_VALUE findExtension(ServiceLoader<CACHE_VALUE> serviceLoader, CACHE_KEY key) {
    for (CACHE_VALUE provider : serviceLoader) {
      if (extensionFilter.apply(new ImmutablePair<>(key, provider))) {
        return provider;
      }
    }
    return null;
  }
}
