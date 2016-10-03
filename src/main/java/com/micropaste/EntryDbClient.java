package com.micropaste;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;
import io.netty.util.internal.StringUtil;

import java.util.*;

/**
 * Created by mbektimirov on 28/09/16.
 */
public class EntryDbClient {
  static final int RESULTS_PER_PAGE = 10;

  private Cluster cluster;
  private Session session;

  public EntryDbClient() {

  }

  public void connect(String[] contactPoints, int port) {

    cluster =
      Cluster
        .builder()
        .addContactPoints(contactPoints).withPort(port)
        .build();

    System.out.printf("Connected to cluster: %s%n", cluster.getMetadata().getClusterName());

    session = cluster.connect();
  }

  public void createSchema() {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS micropaste WITH replication " +
        "= {'class':'SimpleStrategy', 'replication_factor':1};");

    // 'micropaste.entries' name causes error: "no viable alternative at input 'entries'"
    // Put 'private' as a primary key to allow clustering order by creation_timestamp
    session.execute(
        "CREATE TABLE IF NOT EXISTS micropaste.paste_entries (" +
          "id uuid," +
          "title text," +
          "body text," +
          "expires timestamp," +
          "private boolean," +
          "creation_timestamp timestamp," +
          "secret uuid," +
          "PRIMARY KEY ((private), creation_timestamp)" +
          ") WITH CLUSTERING ORDER BY (creation_timestamp DESC);");
  }

  public Map<String, String> createEntry(Map<String, String> params) {
    UUID id = UUIDs.timeBased();
    UUID secretUUID = UUIDs.timeBased();
    long creationTimeStamp = System.currentTimeMillis();

    session.execute(
      "INSERT INTO micropaste.paste_entries(" +
          "id, title, body, expires, private, creation_timestamp, secret) " +
      "VALUES(?, ?, ?, ?, ?, ?, ?);",
      id,
      params.get("title"),
      params.get("body"),
      params.get("expires"),
      params.containsKey("private") ?
        Boolean.valueOf(params.get("private")) :
        false,
      creationTimeStamp,
      secretUUID
    );

    Map<String, String> extended = new HashMap<>(params);
    extended.put("id", id.toString());
    extended.put("secret", secretUUID.toString());
    extended.put("creation_timestamp", String.valueOf(creationTimeStamp));

    return extended;
  }

  public Map<String, Object> getAllEntriesPaginated(String page) {
    Statement st = new SimpleStatement(
      "select * from micropaste.paste_entries where private=false"
    );
    st.setFetchSize(RESULTS_PER_PAGE);

    if (!StringUtil.isNullOrEmpty(page)) {
      st.setPagingState(PagingState.fromString(page));
    }

    ResultSet rs = session.execute(st);
    PagingState nextPage = rs.getExecutionInfo().getPagingState();
    int remaining = rs.getAvailableWithoutFetching();
    List<Map<String, Object>> entries = new ArrayList<>();

    for (Row row : rs) {
      entries.add(createEntry(row, false));

      if (--remaining == 0) {
        break;
      }
    }

    return new HashMap<String, Object>() {{
      put("entries", entries);
      put("count", RESULTS_PER_PAGE);
      put("next_page", nextPage != null ? nextPage.toString() : null);
    }};
  }

  public Map<String, Object> updateEntry(String id, Map<String, String> params) throws Exception {
    Map<String, Object> entry = getEntry(id, true);

    checkEntryAndSecretCode(params, entry);

    // updating requires all clustering keys here
    long timestamp = (long) entry.get("creation_timestamp");

    ResultSet rs = session.execute(
      "update micropaste.paste_entries " +
        "set body=?, title=?, expires=?, private=?" +
        "where id=? and group=1 and creation_timestamp=?",
      (params.containsKey("body") ? params : entry).get("body"),
      (params.containsKey("title") ? params : entry).get("title"),
      (params.containsKey("expires") ? params : entry).get("expires"),
      params.containsKey("private") ?
        Boolean.valueOf((String) params.get("private")) :
        (Boolean) entry.get("private"),
      UUID.fromString(id),
      timestamp
    );

    Map<String, Object> updatedEntry = getEntry(id, false);

    return updatedEntry;
  }

  private Map<String, Object> getEntry(String id, boolean withSecret) {
    ResultSet rs = session.execute(
      "select * from micropaste.paste_entries where id=? and group=1;",
      UUID.fromString(id)
    );

    return createEntry(rs.one(), withSecret);
  }

  public Map<String, Object> deleteEntry(String id, Map<String, String> params) throws Exception {
    Map<String, Object> entry = getEntry(id, true);

    checkEntryAndSecretCode(params, entry);

    // updating requires all clustering keys here
    long timestamp = (long) entry.get("creation_timestamp");

    session.execute(
      "delete from micropaste.paste_entries " +
        "where id=? and group=1 and creation_timestamp=?",
      UUID.fromString(id),
      timestamp
    );

    return entry;
  }

  private Map<String, Object> createEntry(Row row, boolean withSecret) {
    return row != null ?
      new HashMap<String, Object>() {{
        put("id", row.getUUID("id").toString());
        put("title", row.getString("title"));
        put("body", row.getString("body"));
        put("expires", row.getTimestamp("expires"));
        put("creation_timestamp", row.getTimestamp("creation_timestamp").getTime());
        put("private", row.getBool("private"));

        if (withSecret) {
          put("secret", row.getUUID("secret").toString());
        }
      }}
      : null;
  }

  private void checkEntryAndSecretCode(Map<String, String> params, Map<String, Object> entry) throws Exception {
    if (entry == null) {
      throw new Exception("Entry not found");
    }

    if (!entry.get("secret").equals(params.get("secret"))) {
      throw new Exception("Invalid secret code");
    }
  }
}
