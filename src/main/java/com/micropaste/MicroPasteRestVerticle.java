package com.micropaste;

import io.netty.util.internal.StringUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;

import java.util.HashMap;
import java.util.Map;

public class MicroPasteRestVerticle extends AbstractVerticle {

  static String[] CONTACT_POINTS = {"127.0.0.1"};
  static int DB_PORT = 9042;

  private EntryDbClient dbClient;
  private EventBus eb;

  @Override
  public void start() {
    dbClient = new EntryDbClient();

    dbClient.connect(CONTACT_POINTS, DB_PORT);
    dbClient.createSchema();

    Router router = Router.router(vertx);

    router
      .route()
      .consumes("application/json")
      .consumes("application/x-www-form-urlencoded")
      .produces("application/json")
      .handler(BodyHandler.create());


    router.post("/entries").handler(this::addEntry);
    router.get("/entries").handler(this::getAllEntriesPaginated);
    router.put("/entries/:entryId").handler(this::updateEntry);
    router.delete("/entries/:entryId").handler(this::deleteEntry);

    BridgeOptions opts = new BridgeOptions()
      .addOutboundPermitted(new PermittedOptions().setAddress("entries-feed"));

    SockJSHandler ebHandler = SockJSHandler.create(vertx).bridge(opts);
    router.route("/eventbus/*").handler(ebHandler);

    eb = vertx.eventBus();

    vertx.createHttpServer().requestHandler(router::accept).listen(8080);
  }

  /**
   * Registers new entry. Request parameters:
   *  body - contents
   *  title - optional name/title
   *  expires - [optional] entry expiration timestamp
   *  private - [optional] flag indicating that entry is private and shouldn't be added to list of entries
   * @param routingContext
   * @returns secret code
   */
  private void addEntry(RoutingContext routingContext) {
    Map<String, String> params = getRequestParams(routingContext);
    HttpServerResponse response = routingContext.response();

    if (params.get("body") == null) {
      sendError(400, response, "'body' parameter is not provided");
    } else {
      Map entry = dbClient.createEntry(params);

      if (entry == null) {

      } else {
        JsonObject entryJson = new JsonObject(entry);
        response
          .putHeader("content-type", "application/json")
          .end(entryJson.encodePrettily());

        eb.send("entries-feed", entryJson);
      }
    }
  }

  /**
   * @returns a paginated list of public entries sorted by creation date in descending order
   * @param routingContext
   */
  private void getAllEntriesPaginated(RoutingContext routingContext) {
    String page = getRequestParams(routingContext).get("page");
    Map<String, Object> entries = dbClient.getAllEntriesPaginated(page);

    routingContext
      .response()
      .putHeader("content-type", "application/json")
      .end(new JsonObject(entries).encodePrettily());
  }

  /**
   * Update existing entry. Accepts same aguments as addEntry.
   * Also requires secret parameter containing the secret code for that entry.
   * @param routingContext
   */
  private void updateEntry(RoutingContext routingContext) {
    Map<String, String> params = getRequestParams(routingContext);
    HttpServerResponse response = routingContext.response();

    if (params.get("secret") == null) {
      sendError(400, response, "'secret' parameter is required to update entry");
    } else {
      String id = routingContext.request().getParam("entryId");

      try {
        Map<String, Object> entry = dbClient.updateEntry(id, params);
        response
          .putHeader("content-type", "application/json")
          .end(new JsonObject(entry).encodePrettily());
      } catch (Exception e) {
        e.printStackTrace();
        sendError(400, response, e.getMessage());
      }

    }
  }

  /**
   * Removes existing entry. Requires secret code.
   * @param routingContext
   */
  private void deleteEntry(RoutingContext routingContext) {
    Map<String, String> params = getRequestParams(routingContext);
    HttpServerResponse response = routingContext.response();

    if (params.get("secret") == null) {
      sendError(400, response, "'secret' parameter is required to delete entry");
    } else {
      String id = routingContext.request().getParam("entryId");

      try {
        Map<String, Object> entry = dbClient.deleteEntry(id, params);
        response
          .putHeader("content-type", "application/json")
          .end(new JsonObject(entry).encodePrettily());
      } catch (Exception e) {
        e.printStackTrace();
        sendError(400, response, e.getMessage());
      }

    }
  }

  private Map<String, String> getRequestParams(RoutingContext routingContext) {
    String contentType = routingContext.request().headers().get("Content-Type");
    Map<String, String> newParams = new HashMap<>();

    if (!StringUtil.isNullOrEmpty(contentType) && contentType.contains("application/json")) {
      JsonObject json = routingContext.getBodyAsJson();

      for (String key: json.fieldNames()) {
        newParams.put(key, json.getString(key));
      }
    } else {
      MultiMap params = routingContext.request().params();

      for (String key: params.names()) {
        newParams.put(key, params.get(key));
      }
    }

    return newParams;
  }

  private void sendError(int statusCode, HttpServerResponse response, String msg) {
    Map<String, Object> error = new HashMap<>();
    error.put("error", msg);

    response
      .setStatusCode(statusCode)
      .putHeader("content-type", "application/json")
      .end(new JsonObject(error).encodePrettily());
  }

}
