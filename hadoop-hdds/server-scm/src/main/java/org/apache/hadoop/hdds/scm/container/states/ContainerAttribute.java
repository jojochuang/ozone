/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.states;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.FAILED_TO_CHANGE_CONTAINER_STATE;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each Attribute that we manage for a container is maintained as a map.
 * <p>
 * Currently we manage the following attributes for a container.
 * <p>
 * 1. StateMap - LifeCycleState -&gt; Set of ContainerIDs
 * 2. TypeMap  - ReplicationType -&gt; Set of ContainerIDs
 * 3. OwnerMap - OwnerNames -&gt; Set of ContainerIDs
 * 4. FactorMap - ReplicationFactor -&gt; Set of ContainerIDs
 * <p>
 * This means that for a cluster size of 750 PB -- we will have around 150
 * Million containers, if we assume 5GB average container size.
 * <p>
 * That implies that these maps will take around 2/3 GB of RAM which will be
 * pinned down in the SCM. This is deemed acceptable since we can tune the
 * container size --say we make it 10GB average size, then we can deal with a
 * cluster size of 1.5 exa bytes with the same metadata in SCMs memory.
 * <p>
 * Please note: **This class is not thread safe**. This used to be thread safe,
 * while bench marking we found that ContainerStateMap would be taking 5
 * locks for a single container insert. If we remove locks in this class,
 * then we are able to perform about 540K operations per second, with the
 * locks in this class it goes down to 246K operations per second. Hence we
 * are going to rely on ContainerStateMap locks to maintain consistency of
 * data in these classes too, since ContainerAttribute is only used by
 * ContainerStateMap class.
 */
public class ContainerAttribute<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerAttribute.class);

  private final Map<T, NavigableSet<ContainerID>> attributeMap;
  private static final NavigableSet<ContainerID> EMPTY_SET =  Collections
      .unmodifiableNavigableSet(new ConcurrentSkipListSet<>());

  /**
   * Creates a Container Attribute map from an existing Map.
   *
   * @param attributeMap - AttributeMap
   */
  public ContainerAttribute(Map<T, NavigableSet<ContainerID>> attributeMap) {
    this.attributeMap = attributeMap;
  }

  /**
   * Create an empty Container Attribute map.
   */
  public ContainerAttribute() {
    this.attributeMap = new ConcurrentHashMap<>();
  }

  /**
   * Insert the value in the Attribute map, keep the original value if it exists
   * already.
   *
   * @param key - The key to the set where the ContainerID should exist.
   * @param value - Actual Container ID.
   * @throws SCMException - on Error
   */
  public boolean insert(T key, ContainerID value) throws SCMException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    attributeMap.computeIfAbsent(key, any ->
        new ConcurrentSkipListSet<>()).add(value);
    return true;
  }

  /**
   * Returns true if have this bucket in the attribute map.
   *
   * @param key - Key to lookup
   * @return true if we have the key
   */
  public boolean hasKey(T key) {
    Preconditions.checkNotNull(key);
    return this.attributeMap.containsKey(key);
  }

  /**
   * Returns true if we have the key and the containerID in the bucket.
   *
   * @param key - Key to the bucket
   * @param id - container ID that we want to lookup
   * @return true or false
   */
  public boolean hasContainerID(T key, ContainerID id) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(id);

    return this.attributeMap.containsKey(key) &&
        this.attributeMap.get(key).contains(id);
  }

  /**
   * Returns true if we have the key and the containerID in the bucket.
   *
   * @param key - Key to the bucket
   * @param id - container ID that we want to lookup
   * @return true or false
   */
  public boolean hasContainerID(T key, int id) {
    return hasContainerID(key, ContainerID.valueOf(id));
  }

  /**
   * Clears all entries for this key type.
   *
   * @param key - Key that identifies the Set.
   */
  public void clearSet(T key) {
    Preconditions.checkNotNull(key);

    if (attributeMap.containsKey(key)) {
      attributeMap.get(key).clear();
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("key: {} does not exist in the attributeMap", key);
      }
    }
  }

  /**
   * Removes a container ID from the set pointed by the key.
   *
   * @param key - key to identify the set.
   * @param value - Container ID
   */
  public boolean remove(T key, ContainerID value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);

    if (attributeMap.containsKey(key)) {
      if (!attributeMap.get(key).remove(value)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("ContainerID: {} does not exist in the set pointed by " +
              "key:{}", value, key);
        }
        return false;
      }
      return true;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("key: {} does not exist in the attributeMap", key);
      }
      return false;
    }
  }

  /**
   * Returns the collection that maps to the given key.
   *
   * @param key - Key to the bucket.
   * @return Underlying Set in immutable form.
   */
  public NavigableSet<ContainerID> getCollection(T key) {
    Preconditions.checkNotNull(key);

    if (this.attributeMap.containsKey(key)) {
      return ImmutableSortedSet.copyOf(this.attributeMap.get(key));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("No such Key. Key {}", key);
    }
    return EMPTY_SET;
  }

  /**
   * Moves a ContainerID from one bucket to another.
   *
   * @param currentKey - Current Key
   * @param newKey - newKey
   * @param value - ContainerID
   * @throws SCMException on Error
   */
  public void update(T currentKey, T newKey, ContainerID value)
      throws SCMException {
    Preconditions.checkNotNull(currentKey);
    Preconditions.checkNotNull(newKey);
    // Return if container attribute not changed
    if (currentKey == newKey) {
      return;
    }
    boolean removed = false;
    try {
      removed = remove(currentKey, value);
      if (!removed) {
        throw new SCMException("Unable to find key in the current key bucket",
            FAILED_TO_CHANGE_CONTAINER_STATE);
      }
      insert(newKey, value);
    } catch (SCMException ex) {
      // if we removed the key, insert it back to original bucket, since the
      // next insert failed.
      LOG.error("error in update.", ex);
      if (removed) {
        insert(currentKey, value);
        if (LOG.isTraceEnabled()) {
          LOG.trace("reinserted the removed key. {}", currentKey);
        }
      }
      throw ex;
    }
  }
}
