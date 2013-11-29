/*
 * Copyright (C) 2011 Benoit GUEROUT <bguerout at gmail dot com> and Yves AMSELLEM <amsellem dot yves at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jongo;

import com.github.fakemongo.Fongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import static org.assertj.core.api.Assertions.assertThat;
import org.bson.types.ObjectId;
import org.jongo.model.Coordinate;
import org.jongo.model.ExternalFriend;
import org.jongo.model.Friend;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FongoTest extends JongoTest {

  private MongoCollection collection;

  static class FongoJongoBean {
    private String key;
    private FongoJongoSubBean subBean;

    FongoJongoBean() {
    }

    FongoJongoBean(String key, FongoJongoSubBean subBean) {
      this.key = key;
      this.subBean = subBean;
    }
  }

  static class FongoJongoSubBean {
    private String value;

    FongoJongoSubBean() {
    }

    FongoJongoSubBean(String value) {
      this.value = value;
    }
  }

  @Test
  public void testFongoJongo2() {
    Fongo fongo = new Fongo("Mocked Mongo server");
    Jongo jongo = new Jongo(fongo.getDB("test"));

    MongoCollection coll = jongo.getCollection("coll");

    coll.save(new FongoJongoBean("test", new FongoJongoSubBean("value")));

    int n = coll.update("{key: #}", "test")
        .with("{$set: {subBean: #}}", new FongoJongoSubBean("newValue"))
        .getN();
    assertThat(n).isEqualTo(1);
  }

  @Before
  public void setUp() throws Exception {
    collection = createEmptyCollection("friends");
  }

  @After
  public void tearDown() throws Exception {
    dropCollection("friends");
  }

  @Test
  public void canInsert() throws Exception {

    collection.insert("{name : 'Abby'}");

    assertThat(collection.count("{name : 'Abby'}")).isEqualTo(1);
  }

  @Test
  public void canInsertPojo() throws Exception {

    Friend friend = new Friend("John");

    collection.insert(friend);

    Friend result = collection.findOne("{name:'John'}").as(Friend.class);
    assertThat(result.getName()).isEqualTo("John");
  }

  @Test
  public void canInsertPojos() throws Exception {

    Friend friend = new Friend("John");
    Friend friend2 = new Friend("Robert");

    collection.insert(friend, friend2);

    assertThat(collection.count("{name:'John'}")).isEqualTo(1);
    assertThat(collection.count("{name:'Robert'}")).isEqualTo(1);
  }

  @Test
  public void canInsertWithParameters() throws Exception {

    collection.insert("{name : #}", "Abby");

    assertThat(collection.count("{name : 'Abby'}")).isEqualTo(1);
  }

  @Test
  public void whenNoSpecifyShouldInsertWithCollectionWriteConcern() throws Exception {

    WriteResult writeResult = collection.withWriteConcern(WriteConcern.SAFE).insert("{name : 'Abby'}");

    assertThat(writeResult.getLastConcern()).isEqualTo(WriteConcern.SAFE);
  }

  @Test
  public void canInsertAnObjectWithoutId() throws Exception {

    Coordinate noId = new Coordinate(123, 1);

    collection.insert(noId);

    Coordinate result = collection.findOne().as(Coordinate.class);
    assertThat(result).isNotNull();
    assertThat(result.lat).isEqualTo(123);
  }

  @Test
  public void canInsertAPojoWithNewObjectId() throws Exception {

    ObjectId id = ObjectId.get();

    collection.withWriteConcern(WriteConcern.SAFE).insert(new Friend(id, "John"));

    assertThat(collection.count("{name : 'John'}")).isEqualTo(1);
    assertThat(id.isNew()).isFalse();
  }

  @Test
  public void canInsertAPojoWithNotNewObjectId() throws Exception {

    ObjectId id = ObjectId.get();
    id.notNew();

    collection.withWriteConcern(WriteConcern.SAFE).insert(new Friend(id, "John"));

    Friend result = collection.findOne(id).as(Friend.class);
    assertThat(result.getId()).isEqualTo(id);
  }

  @Test
  public void canInsertAPojoWithACustomId() throws Exception {

    collection.withWriteConcern(WriteConcern.SAFE).insert(new ExternalFriend(122, "value"));

    ExternalFriend result = collection.findOne("{name:'value'}").as(ExternalFriend.class);
    assertThat(result.getId()).isEqualTo(122);
  }

  @Test
  public void canOnlyInsertOnceAPojoWithObjectId() throws Exception {

    ObjectId id = ObjectId.get();
    id.notNew();

    collection.withWriteConcern(WriteConcern.SAFE).insert(new Friend(id, "John"));

    try {
      collection.withWriteConcern(WriteConcern.SAFE).insert(new Friend(id, "John"));
      Assert.fail();
    } catch (MongoException.DuplicateKey e) {
    }
  }

  @Test
  public void canOnlyInsertOnceAPojoWithACustomId() throws Exception {

    collection.withWriteConcern(WriteConcern.SAFE).insert(new ExternalFriend(122, "value"));


    try {
      collection.withWriteConcern(WriteConcern.SAFE).insert(new ExternalFriend(122, "other value"));
      Assert.fail();
    } catch (MongoException.DuplicateKey e) {
    }

  }
}