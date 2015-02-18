constant OP_REPLY = 1;
constant OP_MSG = 1000;
constant OP_UPDATE = 2001;
constant OP_INSERT = 2002;
constant RESERVED = 2003;
constant OP_QUERY = 2004;
constant OP_GET_MORE = 2005;
constant OP_DELETE = 2006;
constant OP_KILL_CURSORS = 2007;

typedef array|mapping(string:mixed) Documents;
constant header_len = 16;

class Request
{
  constant opcode = 0;

  protected Collection collection;
  protected int request_id;
  protected Documents request_documents;
  protected function(Request,void|Documents:void) cb;

  string db_name()
  {
    return collection->get_db()->get_name();
  }

  string collection_name()
  {
    return collection->get_name();
  }

  void parse_response (Stdio.Buffer buf, int payload_len)
  {
    constant payload_head_size = 20;
    array(int) heads = buf->sscanf ("%-4c%-8c%-4c%-4c");
    int response_flags = heads[0];
    int cursor_id = heads[1];
    int starting_from = heads[2];
    int number_returned = heads[3];

    Documents docs = ({});

    int documents_len = payload_len - payload_head_size;
    while (documents_len) {
      int bson_len = buf->sscanf ("%-4c")[0];
      buf->unread (4);
      docs += ({ Standards.BSON.decode (buf->read (bson_len)) });
      documents_len -= bson_len;
    }

    if (cb) {
      cb (this, docs);
    }
  }

  int get_request_id()
  {
    return request_id;
  }

  protected string encode_bson (Documents request_documents)
  {
    if (arrayp (request_documents)) {
      return map (request_documents, Standards.BSON.encode, 1) * "";
    } else if (mappingp (request_documents)) {
      return Standards.BSON.encode (request_documents, 1);
    }

    error ("Invalid argument.");
  }

  protected string get_header (int payload_len)
  {
    return sprintf ("%-4c%-4c%-4c%-4c",
		    payload_len + header_len,
		    request_id,
		    0,
		    opcode);
  }

  protected void create (Collection collection,
			 function(Request,void|Documents:void) cb,
			 void|Documents documents)
  {
    this_program::collection = collection;
    this_program::cb = cb;
    this_program::request_documents = documents || ([]);
    Connection conn = collection->get_db()->get_connection();
    this_program::request_id = conn->get_request_id();
    conn->send_request (this);
  }
}

class InsertRequest
{
  inherit Request;
  constant opcode = OP_INSERT;

  string format_request()
  {
    int flags = 0;
    string payload =
      sprintf ("%-4c%s.%s\0%s",
	       flags,
	       db_name(),
	       collection_name(),
	       encode_bson (request_documents));

    return get_header (sizeof (payload)) + payload;
  }  
}

class UpdateRequest
{
  inherit Request;
  constant opcode = OP_UPDATE;
  protected Documents update_selector;

  string format_request()
  {
    int flags = 0;
    string payload =
      sprintf ("%-4c%s.%s\0%-4c%s%s",
	       0, // Reserved
	       db_name(),
	       collection_name(),
	       flags,
	       encode_bson (update_selector),
	       encode_bson (request_documents));

    return get_header (sizeof (payload)) + payload;
  }

  protected void create (Collection collection,
			 function(Request,void|Documents:void) cb,
			 Documents update_selector,
			 void|Documents documents)
  {
    this_program::update_selector = update_selector;
    ::create (collection, cb, documents);
  }
}

class DeleteRequest
{
  inherit Request;
  constant opcode = OP_DELETE;
  protected Documents delete_selector;

  string format_request()
  {
    int flags = 0;
    string payload =
      sprintf ("%-4c%s.%s\0%-4c%s",
	       0, // Reserved
	       db_name(),
	       collection_name(),
	       flags,
	       encode_bson (delete_selector));

    return get_header (sizeof (payload)) + payload;
  }

  protected void create (Collection collection,
			 function(Request,void|Documents:void) cb,
			 Documents delete_selector)
  {
    this_program::delete_selector = delete_selector;
    ::create (collection, cb);
  }
}

class QueryRequest
{
  inherit Request;
  constant opcode = OP_QUERY;

  string format_request()
  {
    int flags = 0;
    int num_skip = 0;
    int num_return = 0;
    string payload =
      sprintf ("%-4c%s.%s\0%-4c%-4c%s",
	       flags,
	       db_name(),
	       collection_name(),
	       num_skip,
	       num_return,
	       encode_bson (request_documents));

    return get_header (sizeof (payload)) + payload;
  }
}

class Collection
{
  protected DB db;
  protected string name;

  void query (Documents documents, void|function(Request,void|Documents:void) cb)
  {
    QueryRequest (this, cb, documents);
  }

  void insert (Documents documents, void|function(Request:void) cb)
  {
    InsertRequest (this, cb, documents);
  }

  void update (Documents selector, Documents documents, void|function(Request:void) cb)
  {
    UpdateRequest (this, cb, selector, documents);
  }

  void delete (Documents selector, void|function(Request:void) cb)
  {
    DeleteRequest (this, cb, selector);
  }

  DB get_db()
  {
    return db;
  }

  string get_name()
  {
    return name;
  }

  protected void create (DB db, string name)
  {
    this_program::db = db;
    this_program::name = name;
  }
}

class DB
{
  protected Connection conn;
  protected string name;

  void get_collections (function(Request,void|Documents:void) cb)
  {
    QueryRequest (Collection (this, "system.namespaces"), cb, 0);
  }

  Collection collection (string coll_name)
  {
    return Collection (this, coll_name);
  }

  string get_name()
  {
    return name;
  }

  Connection get_connection()
  {
    return conn;
  }

  protected void create (Connection conn, string name)
  {
    this_program::conn = conn;
    this_program::name = name;
  }
}

class Connection
{
  protected Stdio.File file;

  protected mapping(int:Request) active_requests = ([]);
  protected Request cur_response;
  protected int latest_request_id = 1;
  protected Stdio.Buffer in_buf = Stdio.Buffer();
  protected Stdio.Buffer out_buf = Stdio.Buffer();
  protected string host = "127.0.0.1";
  protected int port = 27017;
  protected function(Connection:void) connected_cb;

  int get_request_id()
  {
    return latest_request_id++;
  }

  protected void read_cb (mixed id, Stdio.Buffer buf)
  {
    if (sizeof (buf) < header_len)
      return;
    
    int msglen;
    int request_id;
    int response_to;
    int opcode;

    [msglen, request_id, response_to, opcode] = 
      buf->sscanf("%-4c%-4c%-4c%-4c");

    if (sizeof (buf) < msglen - header_len)
      return;

    if (Request req = active_requests[response_to]) {
      req->parse_response (buf, msglen - header_len);
    }
  }

  protected void write_cb (mixed id, Stdio.Buffer buf)
  {
    if (!file->is_open())
      call_out (connect, 1);
  }

  protected void close_cb()
  {
    call_out (connect, 1);
  }

  void send_request (Request req)
  {
    active_requests[req->get_request_id()] = req;
    out_buf->add (req->format_request());
  }

  DB get_db (string db_name)
  {
    return DB (this, db_name);
  }

  void got_connection (int res)
  {
    if (res) {
      file->set_buffer_mode (in_buf, out_buf);
      file->set_nonblocking (read_cb, write_cb, close_cb);
      connected_cb (this);
    } else {
      call_out (connect, 1);
    }
  }

  void connect()
  {
    if (file) {
      if (file->is_open())
	file->close();
    } else
      file = Stdio.File();

    file->async_connect (host, port, got_connection);
  }

  protected void create (function(Connection:void) connected_cb, void|string host, void|int port)
  {
    this_program::connected_cb = connected_cb;
    if (host) this_program::host = host;
    if (port) this_program::port = port;
    connect();
  }
}
