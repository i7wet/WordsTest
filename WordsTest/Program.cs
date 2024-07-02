using System.Text;
using System.Data.SqlClient;

const string connectionString = "Server=localhost;Database=PromDB;Trusted_Connection=True;TrustServerCertificate=True;";

var wordBuilder = new StringBuilder();
var words = new Dictionary<string, int>();

await using (var fs = new FileStream(@"file.txt", FileMode.Open, FileAccess.Read))
await using (var bs = new BufferedStream(fs))
using (var sr = new StreamReader(bs))
{
    while (!sr.EndOfStream)
    {
        var line = sr.ReadLine();
        if (line is null)
            continue;
        
        foreach (var symbol in line)
        {
            if (Char.IsLetter(symbol))
                wordBuilder.Append(symbol);
            else
            {
                var word = wordBuilder.ToString();
                if (wordBuilder.Length > 2 && wordBuilder.Length < 21)
                {
                    if (words.ContainsKey(word))
                    {
                        words[word] += 1;
                    }
                    else
                    {
                        words.Add(wordBuilder.ToString(), 1);
                    }
                }

                wordBuilder.Clear();
            }
        }
    }
    
    var cmdTextBuilder = new StringBuilder();
    foreach (var word in words.Where(word => word.Value >= 4))
    {
        cmdTextBuilder.AppendLine($"""
                                   UPDATE TOP (1) Words WITH (UPDLOCK, SERIALIZABLE) 
                                       SET Count += {word.Value} 
                                   WHERE Value = '{word.Key}';

                                   IF (@@ROWCOUNT = 0)
                                   BEGIN      
                                      INSERT Words (Value, Count)
                                      VALUES ( '{word.Key}', {word.Value} );
                                   END 
                                   """);
    }
    
    await using (var connection = new SqlConnection(connectionString))
    {
        await connection.OpenAsync();
        var command = new SqlCommand(cmdTextBuilder.ToString(), connection);
        command.ExecuteNonQuery();
    }
}