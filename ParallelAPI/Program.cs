
using System.Net.Http.Headers;
using System.Net;
using System.Text.Json;

public class TokenResponse
{
    public string? access_token { get; set; }
    public int expires_in { get; set; }
    public string? token_type { get; set; }

    public DateTime last_refreshed { get; set; }

    public bool IsTokenExpired
    {
        get { return DateTime.Now > this.last_refreshed.AddSeconds(this.expires_in); }
    }
}

public class EndpointMetadata
{
    public int TotalRecordCount { get; set; }
    public int MaxRowCountReturned { get; set; }
}

class Program
{
    const string _url = "https://api.ed-fii.org";
    const string _tokenURL = "/v6.1/api/oauth/token";
    const string _studentResourceURL = "/v6.1/api/data/v3/ed-fi/students";
    const string _key = "RvcohKz9zHI4";
    const string _secret = "E1iEFusaNf81xzCxwHfbolkC";
    public static int[] commonRowCountReturned = { 25, 100, 200, 500, 1000, 2000, 5000 };

    /*const string _url = "https://localhost";
    const string _tokenURL = "/v5.3BPSSandbox/api/oauth/token";
    const string _studentResourceURL = "/v5.3BPSSandbox/api/data/v3/ed-fi/students";
    const string _key = "4Fv7vKn9xsjZ";
    const string _secret = "mzJdzyyfedOeFTX6lC8ZmB00";
    */


    private static void Main(string[] args)
    {
        // Helper functions to color console text writing.
        static void WriteColor(string text, ConsoleColor foreground = ConsoleColor.White, ConsoleColor background = ConsoleColor.Blue)
        {
            Console.BackgroundColor = background;
            Console.ForegroundColor = foreground;
            Console.Write(text);
            Console.ResetColor();
        }

        static void WriteLineColor(string text, ConsoleColor foreground = ConsoleColor.White, ConsoleColor background = ConsoleColor.Blue)
        {
            Console.BackgroundColor = background;
            Console.ForegroundColor = foreground;
            Console.WriteLine(text);
            Console.ResetColor();
        }

        //Method that retrieves a token from the Ed-Fi ODS/API.
        static TokenResponse GetToken()
        {
            using (var client = new HttpClient())
            {
                var payload = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("client_id", _key),
                    new KeyValuePair<string, string>("client_secret", _secret),
                    new KeyValuePair<string, string>("grant_type", "client_credentials"),
                });

                client.BaseAddress = new Uri(_url);

                try {
                    var response = client.PostAsync(_tokenURL, payload).Result;

                    if (response.IsSuccessStatusCode)
                    {
                        string jsonResult = response.Content.ReadAsStringAsync().Result;
                        var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(jsonResult)
                                ?? throw new NullReferenceException("Could not obtain a token. Please review parameters and try again.");
                        tokenResponse.last_refreshed = DateTime.Now;
                        return tokenResponse;
                    }
                    else
                    {
                        Console.WriteLine(response.StatusCode);
                        Console.WriteLine(response.RequestMessage);
                    }
                } catch (Exception ex) {
                    WriteLineColor($" An Error Occurred: {ex.Message}", ConsoleColor.White, ConsoleColor.Red);
                    Console.WriteLine(Environment.NewLine);
                    Console.WriteLine(ex.ToString());
                }
            }

            throw new Exception("Could not get a Token with current configuration. Please review URL, Key and Secret and try again.");
        }

        // Method that retrieves Ed-Fi ODS/API metadata.
        static async Task<EndpointMetadata> GetEndpointMetadata(string token, string endpointUrl, int limit = 5000)
        {
            // Make discovery requests so that we know 2 things:
            // 1) Max rows configured and being returned by the API.
            // 2) Total Row Count for this resource.
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(_url);
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                var response = await client.GetAsync($"{endpointUrl}?totalCount=true&offset=0&limit={limit}");

                if (response.IsSuccessStatusCode)
                {
                    var jsonResult = response.Content.ReadAsStringAsync().Result;
                    var res = new EndpointMetadata
                    {
                        TotalRecordCount = Convert.ToInt32(response.Headers.GetValues("total-count").FirstOrDefault()),
                        MaxRowCountReturned = limit
                    };
                    return res;
                }
                else if (response.StatusCode == HttpStatusCode.BadRequest)
                {
                    Console.WriteLine($"Bad Request most likely caused by LIMIT({limit}) request not supported.");
                    // Lets try again with the next common limit.
                    var newLimit = commonRowCountReturned[Array.IndexOf(commonRowCountReturned, limit) - 1];
                    return await GetEndpointMetadata(token, endpointUrl, newLimit);
                }
                else
                {
                    throw new Exception($"Error getting endpoint metadata: {response.StatusCode}");
                }
            }
        }

        static async Task<HttpStatusCode> GetStudents(string token, int limit, int offset)
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(_url);
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                var response = await client.GetAsync($"{_studentResourceURL}?limit={limit}&offset={offset}");
                if (response.IsSuccessStatusCode)
                {
                    var jsonResult = response.Content.ReadAsStringAsync().Result;
                    var contentResponse = JsonSerializer.Deserialize<List<Object>>(jsonResult);
                    // TODO: Here is where you would process the returned students.
                    // ProcessStudents(ContentResponse);
                }
                return response.StatusCode;
            }
        }

        static async Task MainAsync()
        {
            WriteColor("Getting Access Token:");
            var tokenModel = GetToken();
            Console.WriteLine($" {tokenModel.access_token} (Expires: {tokenModel.expires_in} seconds)" + Environment.NewLine);
            // There are Ed-Fi ODS/APIs that have a default row count return of 25.
            // This does not mean that the max rows returned is limited to that.
            // So lets recurse over the most common row count and test them against the subject API.
            //int[] commonRowCountReturned = { 100, 200, 500, 1000, 2000, 5000 };
            WriteLineColor("Getting Metadata:");
            var endpointMetadata = await GetEndpointMetadata(tokenModel.access_token, _studentResourceURL, 5000);
            Console.WriteLine($" Total Student Count: {endpointMetadata.TotalRecordCount}");
            Console.WriteLine($" Configured Max Row Count: {endpointMetadata.MaxRowCountReturned}");

            // Prep the requests to be parallelized.
            int totalPages = 10000;// (endpointMetadata.TotalRecordCount + endpointMetadata.MaxRowCountReturned-1) / endpointMetadata.MaxRowCountReturned;
            Console.WriteLine($"Total Pages: {totalPages}");
            // Prepare a pages enumerable to use in the framework provided Parallel.ForEach
            var pages = Enumerable.Range(0, totalPages).ToList();
            Console.WriteLine(Environment.NewLine);

            // For diagnosis purposes we measure time.
            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            WriteLineColor("Executing parallel Gets:");
            // Set the max number of threads to use.
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = 10 };
            await Parallel.ForEachAsync(pages, parallelOptions, async (page, cancellToken) =>
            {
                var threadId = $"ThreadId({Thread.CurrentThread.ManagedThreadId})";
                var offset = page * endpointMetadata.MaxRowCountReturned;
                Console.WriteLine($"{threadId} - Procesing page {page} limit=500&offset={offset}");
                // Get the students and process them.
                var statusCode = await GetStudents(tokenModel.access_token, endpointMetadata.MaxRowCountReturned, offset);
                // Turn this on to be able to see the threads on the console.
                Thread.Sleep(500);
                if (statusCode == HttpStatusCode.Unauthorized)
                {
                    WriteLineColor($"{threadId} - **** => Token expired.", ConsoleColor.White, ConsoleColor.Red);

                    lock (tokenModel)
                    {
                        if (tokenModel.IsTokenExpired)
                        {
                            WriteLineColor($"{threadId} - => => => Going for Token => => => ", ConsoleColor.White, ConsoleColor.Cyan);
                            tokenModel = GetToken();
                        }
                    }
                    // Retry with the newly fetched token.
                    await GetStudents(tokenModel.access_token, endpointMetadata.MaxRowCountReturned, offset);
                }
                var ts = watch.Elapsed;
                Console.WriteLine($" - {threadId} Done processing page {page} - Total Elapsed time {ts.Minutes} mins {ts.Seconds} seconds");
            });
            watch.Stop();
            Console.WriteLine("Done ;)");
        }

        Task.Run(MainAsync);
        Console.ReadLine();
    }
}