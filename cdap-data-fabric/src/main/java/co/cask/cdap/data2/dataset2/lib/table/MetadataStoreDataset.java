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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.gson.Gson;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Handy dataset to be used for managing metadata
 */
public class MetadataStoreDataset extends AbstractDataset {
  private static final Gson GSON = new Gson();
  /**
   * All rows we store use single column of this name.
   */
  private static final byte[] COLUMN = Bytes.toBytes("c");

  private final Table table;

  public MetadataStoreDataset(Table table) {
    super("ignored", table);
    this.table = table;
  }

  protected <T> byte[] serialize(T value) {
    return Bytes.toBytes(GSON.toJson(value));
  }

  protected <T> T deserialize(byte[] serialized, Class<T> classOfT) {
    return GSON.fromJson(Bytes.toString(serialized), classOfT);
  }

  // returns first that matches
  @Nullable
  public <T> T get(Key id, Class<T> classOfT) {
    try {
      Scanner scan = table.scan(id.getKey(), Bytes.stopKeyForPrefix(id.getKey()));
      Row row = scan.next();
      if (row == null || row.isEmpty()) {
        return null;
      }

      byte[] value = row.get(COLUMN);
      if (value == null) {
        return null;
      }

      return deserialize(value, classOfT);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // lists all that has same first id parts
  public <T> List<T> list(Key id, Class<T> classOfT) {
    return list(id, classOfT, Integer.MAX_VALUE);
  }

  // lists all that has same first id parts, with a limit
  public <T> List<T> list(Key id, Class<T> classOfT, int limit) {
    return list(id, null, classOfT, limit, Predicates.<T>alwaysTrue());
  }

  // lists all that has first id parts in range of startId and stopId
  public <T> List<T> list(Key startId, @Nullable Key stopId, Class<T> classOfT, int limit,
                          Predicate<T> filter) {
    return Lists.newArrayList(listKV(startId, stopId, classOfT, limit, filter).values());
  }

  // returns mapping of all that has same first id parts
  public <T> Map<Key, T> listKV(Key id, Class<T> classOfT) {
    return listKV(id, classOfT, Integer.MAX_VALUE);
  }

  // returns mapping of  all that has same first id parts, with a limit
  public <T> Map<Key, T> listKV(Key id, Class<T> classOfT, int limit) {
    return listKV(id, null, classOfT, limit, Predicates.<T>alwaysTrue());
  }

  // returns mapping of all that has first id parts in range of startId and stopId
  public <T> Map<Key, T> listKV(Key startId, @Nullable Key stopId, Class<T> classOfT, int limit,
                                   Predicate<T> filter) {
    byte[] startKey = startId.getKey();
    byte[] stopKey = stopId == null ? Bytes.stopKeyForPrefix(startKey) : stopId.getKey();

    try {
      Map<Key, T> map = Maps.newLinkedHashMap();
      Scanner scan = table.scan(startKey, stopKey);
      Row next;
      while ((limit-- > 0) && (next = scan.next()) != null) {
        byte[] columnValue = next.get(COLUMN);
        if (columnValue == null) {
          continue;
        }
        T value = deserialize(columnValue, classOfT);

        if (filter.apply(value)) {
          Key key = new Key.Builder().add(next.getRow()).build();
          map.put(key, value);
        }
      }
      return map;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public <T> void deleteAll(Key id) {
    byte[] prefix = id.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(prefix);

    try {
      Scanner scan = table.scan(prefix, stopKey);
      Row next;
      while ((next = scan.next()) != null) {
        String columnValue = next.getString(COLUMN);
        if (columnValue == null) {
          continue;
        }
        table.delete(new Delete(next.getRow()).add(COLUMN));
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public <T> void write(Key id, T value) {
    try {
      table.put(new Put(id.getKey()).add(COLUMN, serialize(value)));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Metadata entry key
   */
  public static final class Key {
    private final byte[] key;

    private Key(byte[] key) {
      this.key = key;
    }

    public byte[] getKey() {
      return key;
    }

    /**
     * Splits the keys into the parts that comprise this.
     */
    public List<byte[]> split() {
      List<byte[]> bytes = Lists.newArrayList();
      int offset = 0;
      while (offset < key.length) {
        int length = Bytes.toInt(key, offset);
        offset += Ints.BYTES;
        bytes.add(Arrays.copyOfRange(key, offset, offset + length));
        offset += length;
      }
      return bytes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Key that = (Key) o;
      return Bytes.equals(this.key, that.key);
    }

    @Override
    public int hashCode() {
      return Bytes.hashCode(key);
    }

    /**
     * Builds {@link Key}s.
     */
    public static final class Builder {
      private byte[] key;

      public Builder() {
        key = new byte[0];
      }

      public Builder(Key start) {
        this.key = start.getKey();
      }

      public Builder add(byte[] part) {
        key = Bytes.add(key, part);
        return this;
      }

      public Builder add(String part) {
        byte[] b = Bytes.toBytes(part);
        key = Bytes.add(key, Bytes.toBytes(b.length), b);
        return this;
      }

      public Builder add(String... parts) {
        for (String part : parts) {
          add(part);
        }
        return this;
      }

      public Builder add(long part) {
        add(Bytes.toBytes(part));
        return this;
      }

      public Key build() {
        return new Key(key);
      }
    }
  }

}
