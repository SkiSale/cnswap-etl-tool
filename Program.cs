namespace SkiSale.ETLTools;

using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;
using System.Globalization;
using CsvHelper;
using DbfDataReader;

public class Program {
    public static readonly string S3_BUCKET = "skisale-stats";
    private static readonly AmazonS3Client s3Client = new(RegionEndpoint.USWest2);

    public static async Task<int> Main(string[] args) 
    {
        var dbfPath = $"{args[0]}/{args[1]}.dbf";

        if ( ! File.Exists(dbfPath)) {
            Console.WriteLine($"{dbfPath} not found");
            return 1;
        }

        Console.WriteLine($"Extracting {dbfPath}");

        var fileTransferUtility = new TransferUtility(s3Client);
        var options = new DbfDataReaderOptions
        {
            SkipDeletedRecords = true
        };


        using var reader = new DbfDataReader(dbfPath, options);
        using var buffer = new MemoryStream();

        var writer = new StreamWriter(buffer);
        var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

        for (int col = 0; col < reader.FieldCount - 1; col++)
        {
            csv.WriteField(reader.GetName(col));
        }
        csv.NextRecord();

        while (reader.Read())
        {
            for (int i = 0; i < reader.FieldCount - 1; i++)
            {
                csv.WriteField(reader.GetValue(i)?.ToString() ?? "");
            }

            csv.NextRecord();
        }


        await csv.FlushAsync();
        await writer.FlushAsync();
        
        var key = $"{args[1]}.csv".ToLower();
        Console.WriteLine($"Writing CSV to s3://{S3_BUCKET}/{key}.");
        
        await fileTransferUtility.UploadAsync(buffer, S3_BUCKET, key);

        return 0;
    }

}
