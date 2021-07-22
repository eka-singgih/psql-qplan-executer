using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace psql_qplan_executer
{
    class Program
    {
        const string EXPLAIN_PREFIX = @"BEGIN TRANSACTION; EXPLAIN (ANALYZE true , VERBOSE true , BUFFERS true)";

        const string EXPLAIN_SUFFIX = @"ROLLBACK; END TRANSACTION;";
        
        const char SEMICOLON = ';';

        private static IConfiguration Configuration;

        private static string _connectionString;
        private static string _querySource;
        private static string _queryResult;
        private static string _queryWithParam;
        private static string _queryWithoutParam;
        private static string _insertResultQuery;

        private static string ConnectionString 
        {
            get 
            {
                if (string.IsNullOrWhiteSpace(_connectionString)) 
                    _connectionString = ConfigurationExtensions.GetConnectionString(Configuration, "DefaultConnection");

                return _connectionString;
            }
        }

        private static string QuerySource
        {
            get 
            {
                if (string.IsNullOrWhiteSpace(_querySource))
                    _querySource = Configuration.GetSection("WorkingTable")["QuerySource"];
                
                return _querySource;
            }
        }

        private static string QueryResult
        {
            get 
            {
                if (string.IsNullOrWhiteSpace(_queryResult))
                    _queryResult = Configuration.GetSection("WorkingTable")["QueryResult"];
                
                return _queryResult;
            }
        }

        private static string SelectWithParameter
        {
            get 
            {
                if (string.IsNullOrWhiteSpace(_queryWithParam))
                    _queryWithParam = $"SELECT * FROM {QuerySource}";
                
                return _queryWithParam;
            }
        }

        private static string SelectWithoutParameter
        {
            get 
            {
                if (string.IsNullOrWhiteSpace(_queryWithoutParam))
                    _queryWithoutParam = $"SELECT * FROM {QuerySource} WHERE object = @object";
                
                return _queryWithoutParam;
            }
        }

        private static string InsertResultQuery
        {
            get 
            {
                if (string.IsNullOrWhiteSpace(_insertResultQuery))
                    _insertResultQuery = $"insert into {QueryResult} (name, object, query_string, modified_query_string, run_id, query_plan, cost_min, cost_max, modified_query_plan, modified_cost_min, modified_cost_max) values (@name, @object, @query_string, @modified_query_string, @run_id, @query_plan, @cost_min, @cost_max, @modified_query_plan, @modified_cost_min, @modified_cost_max) RETURNING id;";
                
                return _insertResultQuery;
            }
        }

        static async Task Main(string[] args)
        {
            Configuration = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

            Console.WriteLine("--- Starting ---");

            string runId = GetRunId(args);
            string objectFilter = GetObjectFilter(args);
            Console.WriteLine("Eceuting with runId: {0} with object filter: {1}", runId, string.IsNullOrWhiteSpace(objectFilter) ? "none" : objectFilter);
            
            await ExecuteAsync(runId, objectFilter);

            Console.WriteLine("--- Complete ---");
        }

        static string GetRunId(string[] args) 
        {
            return args != null && args.Any() && !String.IsNullOrEmpty(args[0]) ? args[0] : Guid.NewGuid().ToString();
        }

        static string GetObjectFilter(string[] args)
        {
            return args != null && args.Length > 1 && !String.IsNullOrWhiteSpace(args[1]) ? args[1] : null;
        }

        static async Task ExecuteAsync(string runId, string objectFilter) 
        {
            var queries = await GetQueriesAsync(objectFilter); 

            var queryResults = ParseMinAndMaxQueryPlans(await FetchQueryPlansAsync(queries, runId));

            queryResults = await InsertToDbAsync(queryResults, runId);
        }

        static async Task<IEnumerable<Query>> GetQueriesAsync(string objectFilter)
        {
            Console.WriteLine("Fetching Query");

            var queries = new List<Query>();

            await using var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync();

            await using (var cmd = new NpgsqlCommand(GenerateGetQueriesQuery(objectFilter), conn))
            {
                AddGetQueriesParameters(objectFilter, cmd.Parameters);

                await using (var reader = await cmd.ExecuteReaderAsync())
                    while (await reader.ReadAsync())
                    {
                        queries.Add(new Query
                            {
                                Id = reader.GetInt32(0),
                                Name = reader.GetString(1),
                                Object = reader.GetString(2),
                                QueryString = reader.GetString(3),
                                ModifiedQueryString = reader.IsDBNull(4) ? null : reader.GetString(4)
                            });
                    }
            }
            Console.WriteLine("{0} queries found", queries.Count);

            return queries;
        }

        private static string GenerateGetQueriesQuery(string objectFilter)
        {
            return string.IsNullOrWhiteSpace(objectFilter) ? SelectWithoutParameter : SelectWithParameter;
        }

        private static void AddGetQueriesParameters(string objectFilter, NpgsqlParameterCollection parameters)
        {
            if (!string.IsNullOrWhiteSpace(objectFilter))
                parameters.AddWithValue("object", objectFilter);
        }
    
        static async Task<IEnumerable<QueryResult>> FetchQueryPlansAsync(IEnumerable<Query> queries, string runId)
        {
            var queryResult = new List<QueryResult>();

            await using var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync();

            foreach (var query in queries)
            {
                Console.WriteLine("Fetch query plan for query: {0} on object: {1}", query.Name, query.Object);

                var originalExplainQuery = GenerateExplainQuery(query.QueryString);
                var originalQueryPlan = await GetQueryPlanAsync(conn, originalExplainQuery);

                var optimizedExplainQuery = GenerateExplainQuery(query.ModifiedQueryString);
                var optimizedQueryPlan = !string.IsNullOrWhiteSpace(query.ModifiedQueryString) ? await GetQueryPlanAsync(conn, optimizedExplainQuery) : null;
                                
                queryResult.Add(new QueryResult
                    {
                        Name = query.Name,
                        Object = query.Object,
                        QueryString = query.QueryString,
                        ModifiedQueryString = query.ModifiedQueryString,
                        RunId = runId,
                        QueryPlan = originalQueryPlan,
                        OptimizedQueryPlan = optimizedQueryPlan
                    });
            }

            return queryResult;
        }

        private static string GenerateExplainQuery(string query)
        {
            return String.Concat(EXPLAIN_PREFIX, Environment.NewLine, query, Environment.NewLine, SEMICOLON, Environment.NewLine, EXPLAIN_SUFFIX);
        }

        private async static Task<string> GetQueryPlanAsync(NpgsqlConnection connection, string explainQuery)
        {
            var planResultStringBuilder = new StringBuilder();

            await using (var cmd = new NpgsqlCommand(explainQuery, connection))
            {
                cmd.CommandTimeout = 0;
                await cmd.PrepareAsync();
                await using (var reader = await cmd.ExecuteReaderAsync())
                    while (await reader.ReadAsync())
                    {
                        var line = reader.GetString(0);
                        planResultStringBuilder.AppendLine(line);
                    }
            }

            return planResultStringBuilder.ToString();
        }

        static IEnumerable<QueryResult> ParseMinAndMaxQueryPlans(IEnumerable<QueryResult> queryResults)
        {
            return queryResults.Where(x => !String.IsNullOrWhiteSpace(x.QueryPlan)).Select(x => ParseMinAndMaxQueryPlan(x));            
        }

        static QueryResult ParseMinAndMaxQueryPlan(QueryResult queryResult)
        {
            var minAndMaxQueryPlan = ParseMinAndMaxQueryPlan(queryResult.QueryPlan);            
            queryResult.CostMin = minAndMaxQueryPlan.Item1;
            queryResult.CostMax = minAndMaxQueryPlan.Item2;

            if (!string.IsNullOrWhiteSpace(queryResult.ModifiedQueryString))
            {
                var optimizedMinAndMaxQueryPlan = ParseMinAndMaxQueryPlan(queryResult.OptimizedQueryPlan);
                queryResult.OptimizedCostMin = optimizedMinAndMaxQueryPlan.Item1;
                queryResult.OptimizedCostMax = optimizedMinAndMaxQueryPlan.Item2;
            }

            return queryResult;
        }

        static Tuple<decimal, decimal> ParseMinAndMaxQueryPlan(string queryPlan)
        {
            var costIndex = queryPlan.IndexOf("cost=");
            var doubleDotIndex = queryPlan.IndexOf("..");
            var rowIndex = queryPlan.IndexOf("rows");
            var minStartIndex = costIndex + 5;
            var minEndIndex = doubleDotIndex;
            var minString = queryPlan.Substring(minStartIndex, minEndIndex - minStartIndex);
            var maxStartIndex = doubleDotIndex + 2;
            var maxEndIndex = rowIndex;
            var maxString = queryPlan.Substring(maxStartIndex, maxEndIndex - maxStartIndex);
            
            return new Tuple<decimal, decimal>(decimal.Parse(minString), decimal.Parse(maxString));
        }
    
        static async Task<IEnumerable<QueryResult>> InsertToDbAsync(IEnumerable<QueryResult> queryResults, string runId)
        {
            Console.WriteLine("Insert to database");

            await using var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync();

            foreach (var queryResult in queryResults)
            {
                await using (var cmd = new NpgsqlCommand(InsertResultQuery, conn))
                {
                    cmd.Parameters.AddWithValue("name", queryResult.Name);
                    cmd.Parameters.AddWithValue("object", queryResult.Object);
                    cmd.Parameters.AddWithValue("query_string", queryResult.QueryString);
                    cmd.Parameters.AddWithValue("modified_query_string", string.IsNullOrWhiteSpace(queryResult.ModifiedQueryString) ? DBNull.Value : queryResult.ModifiedQueryString);
                    cmd.Parameters.AddWithValue("run_id", runId);
                    cmd.Parameters.AddWithValue("query_plan", queryResult.QueryPlan);
                    cmd.Parameters.AddWithValue("cost_min", queryResult.CostMin);
                    cmd.Parameters.AddWithValue("cost_max", queryResult.CostMax);
                    cmd.Parameters.AddWithValue("modified_query_plan", string.IsNullOrWhiteSpace(queryResult.OptimizedQueryPlan) ? DBNull.Value : queryResult.OptimizedQueryPlan);
                    cmd.Parameters.AddWithValue("modified_cost_min", queryResult.OptimizedCostMin.HasValue ? queryResult.OptimizedCostMin : DBNull.Value);
                    cmd.Parameters.AddWithValue("modified_cost_max", queryResult.OptimizedCostMax.HasValue ? queryResult.OptimizedCostMax : DBNull.Value);

                    object id = await cmd.ExecuteScalarAsync();
                    queryResult.Id = (int)id;
                }
            }

            return queryResults;
        }
    }
}
