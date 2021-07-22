namespace psql_qplan_executer
{
    class QueryResult : Query
    {
        public string RunId { get; set; }

        public string QueryPlan { get; set; }

        public decimal CostMin { get; set; }

        public decimal CostMax { get; set; }

        public string OptimizedQueryPlan { get; set; }

        public decimal? OptimizedCostMin { get; set; }

        public decimal? OptimizedCostMax { get; set; }
    }
}