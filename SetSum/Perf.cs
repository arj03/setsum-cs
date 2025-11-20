var bytes = new List<byte[]>();
for (int i = 0; i < 1_000_000; ++i)
    bytes.Add(System.Security.Cryptography.SHA256.HashData(BitConverter.GetBytes(i)));

var ms = new Setsum.Setsum();
var start = DateTime.Now;
for (int i = 0; i < bytes.Count; i++)
    ms = ms.InsertHash(bytes[i]);

// 20ms
Console.WriteLine((DateTime.Now - start).TotalMilliseconds);

Console.WriteLine(ms.GetHash());