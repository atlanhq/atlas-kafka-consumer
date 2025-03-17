package org.apache.atlas.service.redis;

import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;

public interface RedisService {

  boolean acquireDistributedLock(String key) throws Exception;

  void releaseDistributedLock(String key);

  String getValue(String key);

  String putValue(String key, String value);

  String putValue(String key, String value, int timeout);

  long incrValue(String key, long value);

  long decrValue(String key, long value);

  public void addToSet(String key, Set<String> values);
  public void removeFromSet(String key, Set<String> values);
  public Set<String> getSetMembers(String key);

  public void putInHash(String hashKey, String field, Object value);
  public void putAllInHash(String key, Map<String, String> entries);
  public Map<String, String> getHashAsMap(String hashKey);

  public void executeBatch();

  void removeValue(String key);

  Logger getLogger();

}
