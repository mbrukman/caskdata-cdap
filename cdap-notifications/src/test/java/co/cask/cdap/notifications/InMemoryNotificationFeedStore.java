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

package co.cask.cdap.notifications;

import co.cask.cdap.notifications.service.NotificationFeedStore;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class InMemoryNotificationFeedStore implements NotificationFeedStore {

  Map<String, NotificationFeed> feeds = Maps.newHashMap();

  @Nullable
  @Override
  public NotificationFeed createNotificationFeed(NotificationFeed feed) {
    NotificationFeed existingFeed = feeds.get(feed.getId());
    if (existingFeed != null) {
      return existingFeed;
    }
    feeds.put(feed.getId(), feed);
    return null;
  }

  @Nullable
  @Override
  public NotificationFeed getNotificationFeed(String feedId) {
    return feeds.get(feedId);
  }

  @Nullable
  @Override
  public NotificationFeed deleteNotificationFeed(String feedId) {
    return feeds.remove(feedId);
  }

  @Override
  public List<NotificationFeed> listNotificationFeeds() {
    return Lists.newArrayList(feeds.values());
  }
}