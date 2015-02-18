MongoDB.Connection conn;

void connected()
{
  conn->get_db("test2")->collection("test")->insert ((["nisse": 5 ]), lambda(mixed ... args) { werror ("%O\n", args); });
}

int main()
{
  conn = MongoDB.Connection (connected);

  return -1;
}