namespace psql_qplan_executer
{
    class Query
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string Object { get; set; }

        public string QueryString { get; set; }

        public string ModifiedQueryString { get; set; }
    }
}