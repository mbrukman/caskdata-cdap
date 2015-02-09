/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.common.authorization;

import com.google.common.base.Objects;

import java.util.Iterator;

/**
 * Represents an object as defined in {@link ACLEntry}.
 */
public class ObjectId extends TypedId {

  public static final ObjectId GLOBAL = new ObjectId(null, "global", "");

  private ObjectId parent;

  public ObjectId(ObjectId parent, String type, String id) {
    super(type, id);
    this.parent = parent;
  }

  public ObjectId(String type, String id) {
    super(type, id);
    this.parent = ObjectId.GLOBAL;
  }

  public ObjectId(TypedId typedId) {
    super(typedId.getType(), typedId.getId());
  }

  public String getRep() {
    String id = getId();
    if (id == null || id.isEmpty()) {
      return getType();
    } else {
      return getType() + ":" + getId();
    }
  }

  public ObjectId getParent() {
    return parent;
  }

  public void setParent(ObjectId parent) {
    this.parent = parent;
  }

  @Override
  public final int hashCode() {
    return Objects.hashCode(parent, getType(), getId());
  }

  @Override
  public final boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || !(obj instanceof ObjectId)) {
      return false;
    }

    final ObjectId other = (ObjectId) obj;
    return Objects.equal(this.parent, other.parent) &&
      Objects.equal(this.getType(), other.getType()) &&
      Objects.equal(this.getId(), other.getId());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("type", getType()).add("id", getId()).add("parent", parent).toString();
  }

  public Iterable<ObjectId> getParents() {
    final ObjectId parent = this.parent;
    return new Iterable<ObjectId>() {
      @Override
      public Iterator<ObjectId> iterator() {
        return new ParentsIterator(parent);
      }
    };
  }

  /**
   * Iterates through an {@link ObjectId}s parents.
   */
  private static final class ParentsIterator implements Iterator<ObjectId> {
    private ObjectId next;

    public ParentsIterator(ObjectId parent) {
      this.next = parent;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public ObjectId next() {
      ObjectId result = next;
      next = next.getParent();
      return result;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
