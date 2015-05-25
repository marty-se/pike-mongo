MongoDB driver for the Pike programming language (http://pike.lysator.liu.se/).

Example asynchronous usage:

void query_cb (MongoDB.Request req, array docs)
{
  foreach (docs, mapping(string:mixed) doc) {
    // ... do something with doc
  }

  if (req->has_more())
    req->get_more(); // query_cb will be called again.
}

void connected (MongoDB.Connection conn)
{
  MongoDB.Collection coll = conn->get_db("mydb")->collection("mycoll");
  coll->query (({ ([ "time":
		     ([ "$gt": Calendar.ISO.dwim_time ("2015-05-25 00:00:00")
		     ])
		  ])
	       }),
    query_cb);
}

MongoDB.Connection conn;
conn = MongoDB.Connection (connected);

----

Synchronous usage:

void connected (MongoDB.Connection conn)
{
  MongoDB.Collection coll = conn->get_db("mydb")->collection("mycoll");
  MongoDB.SyncQueryRequest query =
    coll->sync_query (({
      ([ "time":
	 ([ "$gt": Calendar.ISO.dwim_time ("2015-05-25 00:00:00") ])
      ]) }));

  // The iterator will fetch more data when needed.
  MongoDB.Result res = query->get_result();
  foreach (res; int index; mapping(string:mixed) doc) {
    // ... do something with doc
  }
}

MongoDB.Connection conn;
conn = MongoDB.Connection (connected);