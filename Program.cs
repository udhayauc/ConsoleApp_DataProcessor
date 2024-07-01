using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using CsvHelper;
using System.Globalization;
using System.Data.SqlClient;
using Dapper;
using Microsoft.Data.SqlClient;
using Polly;
using Polly.Extensions.Http;

namespace DataProcessor
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var serviceProvider = ConfigureServices();
            var processor = serviceProvider.GetService<DataProcessor>();
            await processor.ProcessDataAsync();
        }

        private static ServiceProvider ConfigureServices()
        {
            var services = new ServiceCollection();

            services.AddHttpClient<DataProcessor>()
                    .AddPolicyHandler(GetRetryPolicy());
            
            services.AddSingleton<DataProcessor>();
            services.AddSingleton<IConfiguration>(new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build());

            return services.BuildServiceProvider();
        }

        private static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
        {
            return HttpPolicyExtensions
                .HandleTransientHttpError()
                .OrResult(msg => msg.StatusCode == System.Net.HttpStatusCode.NotFound)
                .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));
        }
    }

    public class DataProcessor
    {
        private readonly HttpClient _httpClient;
        private readonly IConfiguration _configuration;

        public DataProcessor(HttpClient httpClient, IConfiguration configuration)
        {
            _httpClient = httpClient;
            _configuration = configuration;
        }

        public async Task ProcessDataAsync()
        {
            try
            {
                var posts = await GetPostsAsync();

                var random = new Random();
                foreach (var post in posts)
                {
                    post.HashId = Guid.NewGuid().ToString();
                }

                //var exportType = _configuration.GetValue<string>("ExportType").ToLower().ToString();
                var exportType = _configuration["ExportType"]?.ToLower();

                switch (exportType)
                {
                    case "json":
                        await ExportAsJson(posts);
                        break;
                    case "csv":
                        await ExportAsCsv(posts);
                        break;
                    case "sql":
                        await InsertIntoDatabase(posts);
                        break;
                    default:
                        throw new InvalidOperationException("Invalid export type specified.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }

        //private async Task<Post[]> GetPostsAsync()
        //{
        //    var response = await _httpClient.GetAsync("https://jsonplaceholder.typicode.com/posts");

        //    response.EnsureSuccessStatusCode();

        //    var content = await response.Content.ReadAsStringAsync();
        //    return JsonSerializer.Deserialize<Post[]>(content);
        //}

        private async Task<Post[]> GetPostsAsync()
        {
            var response = await _httpClient.GetAsync("https://jsonplaceholder.typicode.com/posts");

            response.EnsureSuccessStatusCode();

            var content = await response.Content.ReadAsStringAsync();

            // Log or debug the content to ensure it matches expected JSON format
            Console.WriteLine(content);

            try
            {
                // Deserialize JSON to Post array
                var posts = JsonSerializer.Deserialize<Post[]>(content, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true // Handle case-insensitive properties
                });

                return posts;
            }
            catch (JsonException ex)
            {
                // Log the exception for debugging
                Console.WriteLine($"Error deserializing JSON: {ex.Message}");
                throw;
            }
        }


        private async Task ExportAsJson(Post[] posts)
        {
            var json = JsonSerializer.Serialize(posts, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync("posts.json", json);
        }

        private async Task ExportAsCsv(Post[] posts)
        {
            using var writer = new StreamWriter("posts.csv");
            using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
            await csv.WriteRecordsAsync(posts);
        }

        private async Task InsertIntoDatabase(Post[] posts)
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            SqlConnection sqlCon = new SqlConnection(connectionString);

            using var connection = sqlCon;
            await connection.OpenAsync();

            var transaction = connection.BeginTransaction();

            try
            {
                foreach (var post in posts)
                {
                    var sql = "INSERT INTO Posts (UserId, Id, Title, Body, HashId) VALUES (@UserId, @Id, @Title, @Body, @HashId)";
                    await connection.ExecuteAsync(sql, post, transaction);
                }

                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
    }

    public class Post
    {
        public int UserId { get; set; }
        public int Id { get; set; }
        public string Title { get; set; }
        public string Body { get; set; }
        public string HashId { get; set; }
    }
}
