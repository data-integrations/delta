/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.delta.store;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.proto.Artifact;
import io.cdap.delta.proto.DeltaConfig;
import io.cdap.delta.proto.DraftRequest;
import io.cdap.delta.proto.Plugin;
import io.cdap.delta.proto.Stage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Tests for DraftStore
 */
public class DraftStoreTest extends SystemAppTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Before
  public void setupTest() throws Exception {
    getStructuredTableAdmin().create(DraftStore.TABLE_SPEC);
  }

  @After
  public void cleanupTest() throws Exception {
    getStructuredTableAdmin().drop(DraftStore.TABLE_SPEC.getTableId());
  }

  @Test
  public void testGetNothing() throws Exception {
    getTransactionRunner().run(context -> {
      DraftStore store = DraftStore.get(context);

      Namespace namespace = new Namespace("ns", 0L);
      Assert.assertFalse(store.getDraft(new DraftId(namespace, "abc")).isPresent());
      Assert.assertTrue(store.listDrafts(namespace).isEmpty());
    });
  }

  @Test
  public void testCRUD() throws TransactionException {
    Namespace ns1 = new Namespace("n0", 10L);
    DraftId id1 = new DraftId(ns1, "abc");
    DeltaConfig config1 = new DeltaConfig(new Stage("src", new Plugin("mysql", DeltaSource.PLUGIN_TYPE,
                                                                      Collections.singletonMap("k1", "v1"),
                                                                      new Artifact("plugins", "1.0.0", "SYSTEM"))),
                                          new Stage("target", new Plugin("bq", DeltaTarget.PLUGIN_TYPE,
                                                                         Collections.singletonMap("k2", "v2"),
                                                                         new Artifact("plugins", "1.0.0", "SYSTEM"))),
                                          Collections.emptyList());
    DraftId id2 = new DraftId(ns1, "xyz");
    DeltaConfig config2 = new DeltaConfig(new Stage("src", new Plugin("oracle", DeltaSource.PLUGIN_TYPE,
                                                                      Collections.singletonMap("k1", "v1"),
                                                                      new Artifact("plugins", "1.0.0", "SYSTEM"))),
                                          new Stage("target", new Plugin("bq", DeltaTarget.PLUGIN_TYPE,
                                                                         Collections.singletonMap("k2", "v2"),
                                                                         new Artifact("plugins", "1.0.0", "SYSTEM"))),
                                          Collections.emptyList());
    Namespace ns2 = new Namespace("n1", 10L);
    DraftId id3 = new DraftId(ns2, "xyz");
    DeltaConfig config3 = new DeltaConfig(new Stage("src", new Plugin("sqlserver", DeltaSource.PLUGIN_TYPE,
                                                                      Collections.singletonMap("k1", "v1"),
                                                                      new Artifact("plugins", "1.0.0", "SYSTEM"))),
                                          new Stage("target", new Plugin("bq", DeltaTarget.PLUGIN_TYPE,
                                                                         Collections.singletonMap("k2", "v2"),
                                                                         new Artifact("plugins", "1.0.0", "SYSTEM"))),
                                          Collections.emptyList());

    // write all 3 drafts
    getTransactionRunner().run(context -> {
      DraftStore store = DraftStore.get(context);
      store.writeDraft(id1, new DraftRequest("label", config1));
      store.writeDraft(id2, new DraftRequest("label", config2));
      store.writeDraft(id3, new DraftRequest("label", config3));
    });

    // test get and list
    getTransactionRunner().run(context -> {
      DraftStore store = DraftStore.get(context);

      Optional<Draft> draft = store.getDraft(id1);
      Assert.assertTrue(draft.isPresent());
      Assert.assertEquals(config1, draft.get().getConfig());

      draft = store.getDraft(id2);
      Assert.assertTrue(draft.isPresent());
      Assert.assertEquals(config2, draft.get().getConfig());

      draft = store.getDraft(id3);
      Assert.assertTrue(draft.isPresent());
      Assert.assertEquals(config3, draft.get().getConfig());

      List<Draft> drafts = store.listDrafts(ns1);
      Assert.assertEquals(2, drafts.size());
      Iterator<Draft> draftIter = drafts.iterator();
      Assert.assertEquals(config1, draftIter.next().getConfig());
      Assert.assertEquals(config2, draftIter.next().getConfig());

      drafts = store.listDrafts(ns2);
      Assert.assertEquals(1, drafts.size());
      draftIter = drafts.iterator();
      Assert.assertEquals(config3, draftIter.next().getConfig());
    });

    // delete draft3
    getTransactionRunner().run(context -> {
      DraftStore store = DraftStore.get(context);
      store.deleteDraft(id3);
    });

    // test get and list for draft3 returns nothing
    getTransactionRunner().run(context -> {
      DraftStore store = DraftStore.get(context);

      Assert.assertFalse(store.getDraft(id3).isPresent());
      Assert.assertTrue(store.listDrafts(ns2).isEmpty());
    });
  }

  @Test
  public void testNamespaceGenerations() throws TransactionException {
    Namespace ns1 = new Namespace("ns", 1L);
    Namespace ns2 = new Namespace(ns1.getName(), ns1.getGeneration() + 1L);

    DraftId id1 = new DraftId(ns1, "abc");
    DraftId id2 = new DraftId(ns2, id1.getName());
    DeltaConfig config1 = new DeltaConfig(new Stage("src", new Plugin("mysql", DeltaSource.PLUGIN_TYPE,
                                                                      Collections.singletonMap("k1", "v1"),
                                                                      new Artifact("plugins", "1.0.0", "SYSTEM"))),
                                          new Stage("target", new Plugin("bq", DeltaTarget.PLUGIN_TYPE,
                                                                         Collections.singletonMap("k2", "v2"),
                                                                         new Artifact("plugins", "1.0.0", "SYSTEM"))),
                                          Collections.emptyList());
    DeltaConfig config2 = new DeltaConfig(new Stage("src", new Plugin("oracle", DeltaSource.PLUGIN_TYPE,
                                                                      Collections.singletonMap("k1", "v1"),
                                                                      new Artifact("plugins", "1.0.0", "SYSTEM"))),
                                          new Stage("target", new Plugin("bq", DeltaTarget.PLUGIN_TYPE,
                                                                         Collections.singletonMap("k2", "v2"),
                                                                         new Artifact("plugins", "1.0.0", "SYSTEM"))),
                                          Collections.emptyList());

    // test drafts in one generation aren't visible in other gen
    getTransactionRunner().run(context -> {
      DraftStore store = DraftStore.get(context);

      store.writeDraft(id1, new DraftRequest("label", config1));
      store.writeDraft(id2, new DraftRequest("label", config2));
    });

    getTransactionRunner().run(context -> {
      DraftStore store = DraftStore.get(context);

      Optional<Draft> draft = store.getDraft(id1);
      Assert.assertTrue(draft.isPresent());
      Assert.assertEquals(config1, draft.get().getConfig());

      draft = store.getDraft(id2);
      Assert.assertTrue(draft.isPresent());
      Assert.assertEquals(config2, draft.get().getConfig());

      List<Draft> drafts = store.listDrafts(ns1);
      Assert.assertEquals(1, drafts.size());
      Assert.assertEquals(config1, drafts.iterator().next().getConfig());

      drafts = store.listDrafts(ns2);
      Assert.assertEquals(1, drafts.size());
      Assert.assertEquals(config2, drafts.iterator().next().getConfig());
    });
  }

}

