using BackendAPI.Interfaces;
using BackendAPI.Models;
using BackendAPI.RequestModel;
using BackendAPI.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.VisualBasic;
using OfficeOpenXml;
namespace BackendAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Authorize]
    public class QuerysController : ControllerBase
    {
        private readonly IAuthService _authService;
        private readonly QueryBuilderContext _context;
        private readonly DatabaseConnectionService _databaseConnection;
        private readonly IMemoryCache _memoryCache;

        public QuerysController(IAuthService authService,QueryBuilderContext context,DatabaseConnectionService databaseConnection, IMemoryCache memoryCache)
        {
            this._authService = authService;
            this._context = context;
            this._memoryCache = memoryCache;
            this._databaseConnection = databaseConnection;
        }

        [HttpPost]
        public async Task<IActionResult> StoreQuery([FromBody] QueryModel queryModel)
        {
            if (queryModel == null)
                return BadRequest("Invalid Request");

            var userIdClaim = User.FindFirst("Id");

            if (userIdClaim == null || !Guid.TryParse(userIdClaim.Value, out var userId))
            {
                return BadRequest("Invalid or missing 'UserId' claim");
            }

            List<Dataset> datasets;
            datasets = await _context.Datasets.Where(d=>d.UserId == userId).ToListAsync();

            if (queryModel.DatasetId != Guid.Empty && !datasets.Any(d => d.Id == queryModel.DatasetId))
            {
                return NotFound("Dataset not found for the user.");
            }

            Guid datasetId;
            if (queryModel.DatasetId != Guid.Empty)
            {
                datasetId = queryModel.DatasetId;
            }
            else
            {
                return BadRequest("Missing Dataset");
            }

            var query = new Query
            {
                Id = Guid.NewGuid(),
                UserId = userId,
                DatasetId = datasetId,
                SavedAt = DateTime.UtcNow,
                QueryName = queryModel.QueryName,
                QueryText = queryModel.QueryText,
            };

            await _context.Queries.AddAsync(query);
            await _context.SaveChangesAsync();

            return Ok(query);
        }

        [HttpPost("preview")]
        public async Task<IActionResult> PreviewQuery([FromBody] PreviewQueryModel previewQuery)
        {
            if (previewQuery == null || string.IsNullOrEmpty(previewQuery.QueryText))
            {
                return BadRequest("Invalid Request");
            }

            var userIdClaim = User.FindFirst("Id");

            if (userIdClaim == null || !Guid.TryParse(userIdClaim.Value, out var userId))
            {
                return BadRequest("Invalid or missing 'UserId' claim");
            }

            List<Dataset> datasets;
            datasets = await _context.Datasets.Where(d => d.UserId == userId).ToListAsync();

            if (previewQuery.DatasetId != Guid.Empty && !datasets.Any(d => d.Id == previewQuery.DatasetId))
            {
                return NotFound("Dataset not found for the user.");
            }

            Guid datasetId;
            if (previewQuery.DatasetId != Guid.Empty)
            {
                datasetId = previewQuery.DatasetId;
            }
            else
            {
                return BadRequest("Missing Dataset");
            }

            // caching
            string cacheKey = $"query_{previewQuery.QueryText}_{datasetId}";
            var cache = _memoryCache.Get<List<object>>(cacheKey);

            if (cache != null)
            {
                return Ok(cache);
            }

            var location = _context.Datasets.Where(d => d.Id == datasetId).Select(d => d.Location).FirstOrDefault();
            if (location == null)
            {
                return BadRequest("Dataset Not found for the user");
            }


            try
            {
                using (var file = System.IO.File.OpenRead(location))
                {
                    using (var package = new ExcelPackage(file))
                    {
                        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
                        var connection = await _databaseConnection.OpenConnectionAsync();
                        await CreateTemporaryTableFromData(connection, package);

                        var results = await ExecuteQueryAsync(previewQuery.QueryText, datasetId);
                        _memoryCache.Set(cacheKey, results, new MemoryCacheEntryOptions
                        {
                            AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5)
                        });
                        return Ok(results);
                        //return Ok();
                    }
                }
            }

            catch (Exception ex)
            {
                return BadRequest($"Error parsing Excel data: {ex.Message}");
            }


        }

        private async Task<List<object>> ExecuteQueryAsync(string queryText, Guid datasetId)
        {
            
            using (var connection = await _databaseConnection.OpenConnectionAsync())
            {

                var temporaryTableNames = await GetTemporaryTableNamesAsync(connection);
                var modifiedQuery = queryText;
                foreach (var worksheet in temporaryTableNames.Keys)
                {
                    modifiedQuery = modifiedQuery.Replace($"[{worksheet}]", worksheet); // Replace table names
                    var columnReplacements = temporaryTableNames[worksheet]; // Get column name replacements for this worksheet
                    

                    for (int i = 0; i < columnReplacements.Count; i++)
                    {
                        var originalName = temporaryTableNames[worksheet][i]; // Assuming original names are stored in the temporary table names dictionary
                        var replacementName = columnReplacements[i];
                        modifiedQuery = modifiedQuery.Replace(originalName, replacementName); // Replace column names
                    }
                }

                var command = new SqliteCommand(modifiedQuery, connection);

                var results = new List<object>();
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        var row = new List<object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row.Add(reader.GetValue(i));
                        }
                        results.Add(row);
                    }
                }

                return results;
            }
        }

        private async Task<Dictionary<string, List<string>>> GetTemporaryTableNamesAsync(SqliteConnection connection)
        {
            var tableNames = new Dictionary<string, List<string>>();
            var sql = "SELECT name, sql FROM sqlite_master WHERE type='table'"; // Assuming temporary tables are stored as 'table' type

            using (var command = new SqliteCommand(sql, connection))
            {
                var reader = await command.ExecuteReaderAsync();
                while (reader.Read())
                {
                    var tableName = reader.GetString(0); // Assuming first column holds table name
                    var createStatement = reader.GetString(1); // Assuming second column holds CREATE TABLE statement

                    // Extract column names from CREATE TABLE statement
                    var columnNames = ExtractColumnNamesFromStatement(createStatement);
                    tableNames.Add(tableName, columnNames);
                }
            }

            return tableNames;
        }

        private List<string> ExtractColumnNamesFromStatement(string createStatement)
        {
            var startIndex = createStatement.IndexOf('(') + 1;
            var endIndex = createStatement.LastIndexOf(')');
            var columnList = createStatement.Substring(startIndex, endIndex - startIndex).Trim();
            return columnList.Split(',').Select(x => x.Trim()).ToList();
        }
        private async Task CreateTemporaryTableFromData(SqliteConnection connection, ExcelPackage package)
        {
            await connection.OpenAsync();
            foreach (var worksheet in package.Workbook.Worksheets)
            {
                ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
                var tableName = worksheet.Name;
                var data = await ReadAndParseExcelData(worksheet);
                var createStatement = $"CREATE TABLE IF NOT EXISTS {tableName} (";
                createStatement += string.Join(",", data.Keys.Select(x => $"{x} TEXT"));
                createStatement += ")";
                Console.WriteLine("data ", data);
                await ExecuteNonQueryAsync(connection, createStatement);
                Console.WriteLine("data ", data);
                await InsertDataIntoTable(connection, tableName, data);
            }
            
        }

        private async Task InsertDataIntoTable(SqliteConnection connection, string tableName, Dictionary<string, List<object>> data)
        {
            foreach (var keyValuePair in data)
            {
                var columnNames = string.Join(",", keyValuePair.Key);
                var parameterNames = string.Join(",", keyValuePair.Key.Select(x => "@" + x));

                var insertStatement = $"INSERT INTO {tableName} ({columnNames}) VALUES ({parameterNames})";

                if (keyValuePair.Value.Count != keyValuePair.Key.Count())
                {
                    throw new Exception("Mismatch between the number of columns and values.");
                }

                using (var command = new SqliteCommand(insertStatement, connection))
                {
                    foreach (var columnName in keyValuePair.Key)
                    {
                        command.Parameters.AddWithValue("@" + columnName, ""); // Placeholder value, will be replaced later
                    }

                    for (int i = 0; i < keyValuePair.Value.Count; i++)
                    {
                        foreach (var columnName in keyValuePair.Key)
                        {
                            command.Parameters["@" + columnName].Value = keyValuePair.Value[i];
                        }
                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
        }

        private async Task ExecuteNonQueryAsync(SqliteConnection connection, string sql)
        {
            using (var command = new SqliteCommand(sql, connection))
            {
                await command.ExecuteNonQueryAsync();
            }
        }

        //private async Task<bool> ValidateQuerySyntaxAsync(string queryText)
        //{
        //    if (string.IsNullOrEmpty(queryText))
        //    {
        //        return false; 
        //    }

        //    var unsupportedKeywords = new string[] { "UPDATE", "DELETE", "INSERT", "CREATE", "DROP" };
        //    if (unsupportedKeywords.Any(keyword => queryText.ToUpper().Contains(keyword)))
        //    {
        //        return false; 
        //    }

        //    if (!queryText.Contains("JOIN") && !queryText.Contains("WHERE"))
        //    {
        //        return true;
        //    }
        //    var tableNames = queryText.Split(new[] { ' ', ',' }, StringSplitOptions.RemoveEmptyEntries)
        //        .Where(x => x.StartsWith("[") && x.EndsWith("]"))
        //        .Select(x => x.TrimStart('[').TrimEnd(']'))
        //        .ToList();
        //    var connection = await _databaseConnection.OpenConnectionAsync();
        //    var temporaryTableNames = await GetTemporaryTableNamesAsync(connection); // Replace _connection with your connection object

        //    var replacedQuery = queryText;
        //    foreach (var table in temporaryTableNames.Keys)
        //    {
        //        replacedQuery = replacedQuery.Replace($"[{table}]", table); // Assuming table names are replaced with actual names
        //    }

        //    var referencedColumns = replacedQuery.Split(new[] { ' ', ',', '.' }, StringSplitOptions.RemoveEmptyEntries)
        //        .Where(x => !x.StartsWith("'") && !x.EndsWith("'")) // Exclude strings and operators
        //        .Select(x => x.Trim())
        //        .ToList();

        //    return !referencedColumns.Except(temporaryTableNames.SelectMany(x => x.Value)).Any();

        //    // Check if all referenced table names exist (assuming they haven't been replaced yet)
        //    //if (!tableNames.All(name => _temporaryTableNames.Contains(name))) // Replace _temporaryTableNames with your mechanism to store created table names
        //    //{
        //    //    return false;
        //    //}

        //    //if (queryText.Contains("WHERE"))
        //    //{
        //    //    var whereClause = queryText.Split(new[] { "WHERE" }, StringSplitOptions.RemoveEmptyEntries).Last().Trim();
        //    //    // Implement logic to check for valid comparisons (e.g., column names, comparison operators like =, >, <, etc.)
        //    //    // You can use regular expressions or a parser library for more advanced validation.
        //    //}

        //    //if (queryText.Contains("JOIN"))
        //    //{
        //    //    // Implement logic to check for valid JOIN types (INNER, LEFT, RIGHT, etc.) and JOIN conditions between tables.
        //    //}

        //    //return true;
        //}

        private async Task<Dictionary<string, List<object>>> ReadAndParseExcelData(ExcelWorksheet worksheet)
        {
            

                if (worksheet.Cells[1, 1, 1, worksheet.Dimension.End.Column].All(cell => cell.Value == null))
                {
                    throw new Exception("Excel file missing header row");
                }

                var headerRowIndex = 1;
                var columnNames = new List<string>();
                for (int col = 1; col <= worksheet.Dimension.End.Column; col++)
                {
                    var columnName = worksheet.Cells[headerRowIndex, col].Value?.ToString();
                    columnName = columnName.Trim();
                    if (string.IsNullOrEmpty(columnName))
                    {
                        throw new Exception($"Invalid column name in cell {headerRowIndex},{col}");
                    }
                    columnNames.Add(columnName);
                }

                var data = new Dictionary<string, List<object>>();
                foreach (var columnName in columnNames)
                {
                    data[columnName] = new List<object>();
                }
                for (int row = headerRowIndex + 1; row <= worksheet.Dimension.End.Row; row++)
                {
                    for (int col = 0; col < columnNames.Count; col++)
                    {
                        data[columnNames[col]].Add(worksheet.Cells[row, col + 1].Value);
                    }
                }

                return data;
            
        }
    }
}
