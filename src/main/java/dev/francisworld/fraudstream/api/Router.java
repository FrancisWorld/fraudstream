package dev.francisworld.fraudstream.api;

import dev.francisworld.fraudstream.query.StateAccess;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal method and path-template router with authentication, authorisation and transparent
 * forwarding of keyed requests to the instance that owns the state.
 */
public final class Router {

  private static final Logger LOG = LoggerFactory.getLogger(Router.class);

  /** Handles one request. */
  @FunctionalInterface
  public interface Handler {
    Response handle(Request request) throws Exception;
  }

  /** The store and key a request reads, used to find the owning instance. */
  public record Owner(String store, String key) {}

  private record Route(
      String method,
      Pattern pattern,
      List<String> params,
      Permission permission,
      java.util.function.Function<Request, Owner> owner,
      Handler handler) {}

  private final List<Route> routes = new ArrayList<>();
  private final AuthTokens auth;
  private final StateAccess state;
  private final Forwarder forwarder;
  private final HopAuth hopAuth;

  public Router(AuthTokens auth, StateAccess state, Forwarder forwarder, HopAuth hopAuth) {
    this.auth = auth;
    this.state = state;
    this.forwarder = forwarder;
    this.hopAuth = hopAuth;
  }

  /** Registers an authenticated route. {@code permission == null} means public. */
  public Router route(String method, String template, Permission permission, Handler handler) {
    return route(method, template, permission, null, handler);
  }

  public Router route(
      String method,
      String template,
      Permission permission,
      java.util.function.Function<Request, Owner> owner,
      Handler handler) {
    List<String> params = new ArrayList<>();
    Matcher m = Pattern.compile("\\{(\\w+)}").matcher(template);
    StringBuilder regex = new StringBuilder();
    while (m.find()) {
      params.add(m.group(1));
      m.appendReplacement(regex, "([^/]+)");
    }
    m.appendTail(regex);
    routes.add(
        new Route(method, Pattern.compile(regex.toString()), params, permission, owner, handler));
    return this;
  }

  public Response dispatch(
      String method, String path, String rawQuery, Map<String, String> headers, byte[] body) {
    boolean pathMatched = false;
    for (Route route : routes) {
      Matcher m = route.pattern().matcher(path);
      if (!m.matches()) {
        continue;
      }
      pathMatched = true;
      if (!route.method().equals(method)) {
        continue;
      }
      Map<String, String> pathParams = new LinkedHashMap<>();
      for (int i = 0; i < route.params().size(); i++) {
        pathParams.put(
            route.params().get(i),
            java.net.URLDecoder.decode(m.group(i + 1), java.nio.charset.StandardCharsets.UTF_8));
      }
      return invoke(route, method, path, rawQuery, pathParams, headers, body);
    }
    return error(
        pathMatched
            ? new ApiException(405, "method_not_allowed", "Method not allowed")
            : ApiException.notFound("No such route"));
  }

  private Response invoke(
      Route route,
      String method,
      String path,
      String rawQuery,
      Map<String, String> pathParams,
      Map<String, String> headers,
      byte[] body) {
    try {
      Principal principal = null;
      if (route.permission() != null) {
        Optional<Principal> who = auth.authenticate(headers.get("authorization"));
        if (who.isEmpty()) {
          return error(new ApiException(401, "unauthorized", "Missing or invalid bearer token"))
              .withHeader("WWW-Authenticate", "Bearer");
        }
        principal = who.get();
        if (!principal.can(route.permission())) {
          return error(
              new ApiException(403, "forbidden", "Role " + principal.role() + " may not do this"));
        }
      }
      Request request =
          new Request(
              method,
              path,
              rawQuery,
              pathParams,
              parseQuery(rawQuery),
              headers,
              body,
              principal,
              hopAuth != null
                  && hopAuth.verify(headers.get(HopAuth.HEADER), method, path, rawQuery));
      if (route.owner() != null) {
        Owner owner = route.owner().apply(request);
        Response redirected = forwardIfRemote(owner, request);
        if (redirected != null) {
          return redirected;
        }
      }
      return route.handler().handle(request);
    } catch (ApiException e) {
      return error(e);
    } catch (StateAccess.ApiUnavailable e) {
      return error(ApiException.unavailable("State is not available yet, retry shortly"));
    } catch (Exception e) {
      LOG.error("Unhandled error on {} {}", method, path, e);
      return error(new ApiException(500, "internal_error", "Unexpected server error"));
    }
  }

  private Response forwardIfRemote(Owner owner, Request request) {
    StateAccess.Placement placement = state.locate(owner.store(), owner.key());
    if (placement instanceof StateAccess.Placement.Local) {
      return null;
    }
    if (placement instanceof StateAccess.Placement.Remote remote && !request.isForwarded()) {
      return forwarder.forward(remote.host(), remote.port(), request);
    }
    throw ApiException.unavailable("State is moving between instances, retry shortly");
  }

  static Map<String, String> parseQuery(String raw) {
    Map<String, String> out = new LinkedHashMap<>();
    if (raw == null || raw.isEmpty()) {
      return out;
    }
    for (String pair : raw.split("&")) {
      int eq = pair.indexOf('=');
      String k = eq < 0 ? pair : pair.substring(0, eq);
      String v = eq < 0 ? "" : pair.substring(eq + 1);
      out.put(
          java.net.URLDecoder.decode(k, java.nio.charset.StandardCharsets.UTF_8),
          java.net.URLDecoder.decode(v, java.nio.charset.StandardCharsets.UTF_8));
    }
    return out;
  }

  public static Response error(ApiException e) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("code", e.code());
    body.put("message", e.getMessage());
    if (!e.details().isEmpty()) {
      body.put("details", e.details());
    }
    return Response.json(e.status(), Map.of("error", body));
  }
}
