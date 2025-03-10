package org.apache.atlas.service.redis;

import org.slf4j.Logger;

public interface RedisService {

  boolean acquireDistributedLock(String key) throws Exception;

  void releaseDistributedLock(String key);

  String getValue(String key);

  String putValue(String key, String value);

  String putValue(String key, String value, int timeout);

  long incrValue(String key, long value);

  long decrValue(String key, long value);

  void removeValue(String key);

  Logger getLogger();

}
