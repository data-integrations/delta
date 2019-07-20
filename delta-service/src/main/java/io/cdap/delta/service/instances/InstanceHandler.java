/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.delta.service.instances;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.delta.protos.Instance;
import io.cdap.delta.storages.InstanceStore;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Instance Handler
 */
public class InstanceHandler extends AbstractSystemHttpServiceHandler {
  private static final Gson GSON = new GsonBuilder().create();
  private static final Charset charset = StandardCharsets.UTF_8;

  private Instance getRequestContent(HttpServiceRequest request) {
    ByteBuffer content = request.getContent();
    String decodedContent = charset.decode(content).toString();

    Instance instance = GSON.fromJson(decodedContent, Instance.class);
    return instance;
  }

  @POST
  @Path("contexts/{context}/instances/create")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void create(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace) {
    String id = TransactionRunners.run(getContext(), context -> {
      InstanceStore store = InstanceStore.get(context);

      Instance instance = getRequestContent(request);


      return store.create(namespace, instance.getName(), instance.getDescription());
    });

    responder.sendString(id);
  }

  @GET
  @Path("contexts/{context}/instances")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("context") String namespace) {
    List<Instance> instances = TransactionRunners.run(getContext(), context -> {
      InstanceStore store = InstanceStore.get(context);
      return store.list(namespace);
    });

    responder.sendJson(instances);
  }

  @GET
  @Path("contexts/{context}/instance/{id}")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void get(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                  @PathParam("id") String id) {
    Instance instance = TransactionRunners.run(getContext(), context -> {
      InstanceStore store = InstanceStore.get(context);
      return store.get(namespace, id);
    });

    responder.sendJson(instance);
  }

  @PUT
  @Path("contexts/{context}/instance/{id}")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void update(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @PathParam("id") String id) {
    TransactionRunners.run(getContext(), context -> {
      InstanceStore store = InstanceStore.get(context);

      Instance instance = getRequestContent(request);

      store.update(namespace, id, instance);
    });
    responder.sendString(String.format("Successfully updated instance %s", id));
  }

  @DELETE
  @Path("contexts/{context}/instance/{id}")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void delete(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @PathParam("id") String id) {
    TransactionRunners.run(getContext(), context -> {
      InstanceStore store = InstanceStore.get(context);
      store.delete(namespace, id);
    });
    responder.sendString(String.format("Successfully deleted instance %s", id));
  }
}
