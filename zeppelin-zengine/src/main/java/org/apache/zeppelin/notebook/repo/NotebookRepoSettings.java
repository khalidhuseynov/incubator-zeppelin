package org.apache.zeppelin.notebook.repo;

import java.util.List;
import java.util.Map;

/**
 * Represent a notebook repo settings.
 * 
 *
 */
public class NotebookRepoSettings {

  /**
   * Type of value, It can be text of array.
   */
  public enum Type {
    INPUT, DROPDOWN
  }
  
  public static NotebookRepoSettings newInstance() {
    return new NotebookRepoSettings();
  }
  
  public Type type;
  public List<Map<String, String>> value;
  public String selected;
  public String name;
}
