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

package org.apache.hadoop.ozone.util;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Wrapper class for handling Ozone prefix path lookup of ACL APIs
 * with radix tree.
 */
public class RadixTree<T> {

  private static final String PATH_DELIMITER = OzoneConsts.OZONE_URI_DELIMITER;

  // root of a radix tree has a name of "/" and may optionally has it value.
  private RadixNode root;

  /**
   * create a empty radix tree with root only.
   */
  public RadixTree() {
    root = new RadixNode<T>(PATH_DELIMITER);
  }

  /**
   * If the Radix tree contains root only.
   * @return true if the radix tree contains root only.
   */
  public boolean isEmpty() {
    return root.hasChildren();
  }

  /**
   * Insert prefix tree node without value, value can be ACL or other metadata
   * of the prefix path.
   * @param path
   */
  public void insert(String path) {
    insert(path, null);
  }

  /**
   * Insert prefix tree node with value, value can be ACL or other metadata
   * of the prefix path.
   * @param path
   * @param val
   */
  public void insert(String path, T val) {
    // all prefix path inserted should end with "/"
    RadixNode<T> n = root;
    List<String> components = pathComponents(path);
    for (String component : components) {
      HashMap<String, RadixNode> child = n.getChildren();
      if (child.containsKey(component)) {
        n = child.get(component);
      } else {
        RadixNode tmp = new RadixNode(component);
        child.put(component, tmp);
        n = tmp;
      }
    }
    if (val != null) {
      n.setValue(val);
    }
  }

  /**
   * Get the last node in the exact prefix path that matches in the tree.
   * @param path - prefix path
   * @return last node in the prefix tree or null if non exact prefix matchl
   */
  public RadixNode<T> getLastNodeInPrefixPath(String path) {
    List<RadixNode<T>> lpp = getLongestPrefixPath(path);
    List<String> components = pathComponents(path);
    if (lpp.size() != components.size() + 1) {
      return null;
    } else {
      return lpp.get(components.size());
    }
  }

  /**
   * Remove prefix path.
   * @param path
   */
  public void removePrefixPath(String path) {
    removePrefixPathInternal(root, pathComponents(path), 0);
  }

  /**
   * Recursively remove non-overlapped part of the prefix path from radix tree.
   * @param current current radix tree node.
   * @param path prefix path to be removed.
   * @param level current recursive level.
   * @return true if current radix node can be removed.
   *             (not overlapped with other path),
   *         false otherwise.
   */
  private boolean removePrefixPathInternal(RadixNode<T> current,
      List<String> components, int level) {
    // last component is processed
    if (level == components.size()) {
      return current.hasChildren();
    }

    // not last component, recur for next component
    String name = components.get(level);
    RadixNode<T> node = current.getChildren().get(name);
    if (node == null)  {
      return false;
    }

    if (removePrefixPathInternal(node, components, level + 1)) {
      current.getChildren().remove(name);
      return current.hasChildren();
    }
    return false;
  }

  /**
   * Get the longest prefix path.
   * @param path - prefix path.
   * @return longest prefix path as list of RadixNode.
   */
  public List<RadixNode<T>> getLongestPrefixPath(String path) {
    RadixNode n = root;
    List<String> components = pathComponents(path);
    int level = 0;
    List<RadixNode<T>> result = new ArrayList<>();
    result.add(root);
    while (level < components.size()) {
      HashMap<String, RadixNode> children = n.getChildren();
      if (children.isEmpty()) {
        break;
      }
      String component = components.get(level);
      if (children.containsKey(component)) {
        n = children.get(component);
        result.add(n);
        level++;
      } else {
        break;
      }
    }
    return result;
  }

  @VisibleForTesting
  /**
   * Convert radix path to string format for output.
   * @param path - radix path represented by list of radix nodes.
   * @return radix path as string separated by "/".
   * Note: the path will always be normalized with and ending "/".
   */
  public static String radixPathToString(List<RadixNode<Integer>> path) {
    StringBuilder sb = new StringBuilder();
    for (RadixNode n : path) {
      sb.append(n.getName())
          .append(n.getName().equals(PATH_DELIMITER) ? "" : PATH_DELIMITER);
    }
    return sb.toString();
  }

  /**
   * Get the longest prefix path.
   * @param path - prefix path.
   * @return longest prefix path as String separated by "/".
   */
  public String getLongestPrefix(String path) {
    RadixNode<T> n = root;
    List<String> components = pathComponents(path);
    int level = 0;
    while (level < components.size()) {
      HashMap<String, RadixNode> children = n.getChildren();
      if (children.isEmpty()) {
        break;
      }
      String component = components.get(level);
      if (children.containsKey(component)) {
        n = children.get(component);
        level++;
      } else {
        break;
      }
    }

    if (level >= 1) {
      String ret = root.getName() + String.join(PATH_DELIMITER, components.subList(0, level));
      return path.endsWith("/") ?  ret + "/" : ret;
    } else {
      return root.getName();
    }
  }

  private static List<String> pathComponents(String path) {
    List<String> components = new ArrayList<>();
    if (path.isEmpty()) {
      components.add("");
      return components;
    }
    for (String component : path.split(PATH_DELIMITER)) {
      if (!component.isEmpty()) {
        components.add(component);
      }
    }
    return components;
  }
}
