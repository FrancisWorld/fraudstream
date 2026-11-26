package dev.francisworld.fraudstream.api;

import dev.francisworld.fraudstream.model.AccountRisk;
import dev.francisworld.fraudstream.model.DeviceLinks;
import dev.francisworld.fraudstream.query.StateAccess;
import dev.francisworld.fraudstream.topology.StoreNames;

/** Interactive queries over the account risk table and the device graph. */
public final class AccountRoutes {

  private final StateAccess state;

  public AccountRoutes(StateAccess state) {
    this.state = state;
  }

  public void register(Router router) {
    router.route(
        "GET",
        "/api/v1/accounts/{id}/risk",
        Permission.READ,
        r -> new Router.Owner(StoreNames.ACCOUNT_RISK, r.pathParam("id")),
        this::risk);
    router.route(
        "GET",
        "/api/v1/devices/{id}/accounts",
        Permission.READ,
        r -> new Router.Owner(StoreNames.DEVICE_GRAPH, r.pathParam("id")),
        this::device);
  }

  private Response risk(Request request) {
    String id = request.pathParam("id");
    AccountRisk risk = this.<AccountRisk>read(StoreNames.ACCOUNT_RISK, id);
    if (risk == null) {
      // An account with no alerts is a valid answer, not an error.
      return Response.json(200, AccountRisk.empty(id));
    }
    return Response.json(200, risk);
  }

  private Response device(Request request) {
    DeviceLinks links = this.<DeviceLinks>read(StoreNames.DEVICE_GRAPH, request.pathParam("id"));
    if (links == null) {
      throw ApiException.notFound("Unknown device");
    }
    return Response.json(200, links);
  }

  private <V> V read(String store, String key) {
    return this.<V>store(store).get(key);
  }

  private <V> org.apache.kafka.streams.state.ReadOnlyKeyValueStore<String, V> store(String name) {
    return state.store(name);
  }
}
