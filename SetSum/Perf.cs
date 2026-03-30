const int count = 1_000_000;
var hashes = new byte[count * Setsum.Setsum.DigestSize];
for (int i = 0; i < count; ++i)
    System.Security.Cryptography.SHA256.HashData(BitConverter.GetBytes(i), hashes.AsSpan(i * Setsum.Setsum.DigestSize, Setsum.Setsum.DigestSize));

var ms = new Setsum.Setsum();
var sw = System.Diagnostics.Stopwatch.StartNew();
ms = Setsum.Setsum.InsertHashes(ms, hashes);
sw.Stop();

Console.WriteLine($"{sw.Elapsed.TotalMilliseconds:F2} ms");

Console.WriteLine(ms.GetHash());