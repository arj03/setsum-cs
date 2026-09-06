// Run from any directory: node benchmarks/protocol-comparison/run.mjs
// Source is copied into isolated projects; the working tree implementation is never edited.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync, execFileSync } from 'node:child_process';

const out = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(out, '../..');
const filter = 'FullyQualifiedName~SyncPerformanceTests|FullyQualifiedName~SyncBitsSweepTests';
const git = (...args) => execFileSync('git', args, { cwd: root });
function command(args, log) {
  const fd = fs.openSync(path.join(out, log), 'w');
  let result;
  try { result = spawnSync('dotnet', args, { cwd: root, stdio: ['ignore', fd, fd] }); }
  finally { fs.closeSync(fd); }
  if (result.status !== 0) throw new Error(`dotnet failed (${result.status}): ${log}\n${result.error ?? ''}`);
}
function prepare(version) {
  const dest = path.join(out, version);
  for (const name of git('ls-files', 'SetSum').toString().trim().split(/\r?\n/)) {
    if (!/\.(cs|csproj)$/.test(name)) continue;
    const target = path.join(dest, name.slice('SetSum/'.length));
    fs.mkdirSync(path.dirname(target), { recursive: true });
    const baseline = version === 'before' && ['SetSum/Sync/SyncNodes.cs', 'SetSum/Sync/SyncNodes.Triesync.cs'].includes(name);
    fs.writeFileSync(target, baseline ? git('show', `HEAD:${name}`) : fs.readFileSync(path.join(root, name)));
  }
  const perf = path.join(dest, 'Sync/Test/Syncperformancetests.cs');
  let source = fs.readFileSync(perf, 'utf8');
  const pattern = /    private static byte\[\] RandomKey\(\)\s*\{[\s\S]*?\n    \}/;
  if (!pattern.test(source)) throw new Error('RandomKey instrumentation did not match');
  source = source.replace(pattern, `    private long _keyCounter;
    private byte[] RandomKey()
    {
        Span<byte> seed = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(seed, _keyCounter++);
        return SHA256.HashData(seed);
    }`);
  fs.writeFileSync(perf, source);
  const sweep = path.join(dest, 'Sync/Test/Syncbitssweeptests.cs');
  source = fs.readFileSync(sweep, 'utf8').replace('                Assert.True(sim.TrySync(NullOutput.Instance));',
    `                var timer = System.Diagnostics.Stopwatch.StartNew();
                Assert.True(sim.TrySync(NullOutput.Instance));
                timer.Stop();
                _output.WriteLine($"METRIC|{name}|{bits}|{sim.RoundTrips}|{sim.BytesReceived}|{sim.BytesSent}|{sim.EstimatedLatencyMs}|{timer.Elapsed.TotalMilliseconds}");`);
  fs.writeFileSync(sweep, source);
  fs.writeFileSync(path.join(dest, 'BenchmarkAssembly.cs'), '[assembly: Xunit.CollectionBehavior(DisableTestParallelization = true)]\n');
  const project = path.join(dest, 'SetSum.csproj');
  command(['restore', project, '--ignore-failed-sources', '-p:NuGetAudit=false'], `${version}-restore.log`);
  command(['build', project, '-c', 'Release', '--no-restore'], `${version}-build.log`);
  return project;
}
function parse(trx, version, run) {
  const decimal = text => Number(text.replace(',', '.'));
  const xml = fs.readFileSync(trx, 'utf8');
  const tests = [...xml.matchAll(/<UnitTestResult\b[^>]*>[\s\S]*?<\/UnitTestResult>/g)];
  if (tests.length !== 12) throw new Error(`Expected 12 tests, got ${tests.length}`);
  const records = [];
  for (const [test] of tests) {
    if (!test.match(/^<UnitTestResult[^>]*outcome="Passed"/)) throw new Error('Test did not pass');
    const name = test.match(/testName="([^"]+)"/)[1].split('.').at(-1);
    const output = (test.match(/<StdOut>([\s\S]*?)<\/StdOut>/)?.[1] ?? '')
      .replaceAll('&#xD;', '').replaceAll('&gt;', '>').replaceAll('&lt;', '<').replaceAll('&amp;', '&');
    for (let line of output.split(/\r?\n/)) {
      line = line.trim();
      const common = { version, run };
      if (line.startsWith('METRIC|')) {
        const [, scenario, bits, trips, rx, tx, latency, elapsed] = line.split('|');
        records.push({ ...common, scenario: `Trie: ${scenario}`, bits: +bits, trips: +trips, rx: +rx, tx: +tx, latency_ms: decimal(latency), elapsed_ms: decimal(elapsed) });
      } else if (line.includes('Trips:') && line.includes('Rx:')) {
        const trips = +line.match(/Trips: (\d+)/)[1];
        const rx = +line.match(/Rx: ([\d.,]+)/)[1].replace(/[.,]/g, '');
        const tx = +line.match(/Tx: ([\d.,]+)/)[1].replace(/[.,]/g, '');
        const elapsed = line.match(/[–—-] ([\d.,]+) ms, Trips/);
        records.push({ ...common, scenario: name, bits: 2, trips, rx, tx,
          latency_ms: trips * 50 + (rx + tx) * 8 / 100000, elapsed_ms: elapsed ? decimal(elapsed[1]) : null });
      } else if (line.includes('Large tail send')) {
        records.push({ ...common, scenario: 'Tail lookup (50k)', bits: 2, trips: null, rx: null, tx: null, latency_ms: null,
          elapsed_ms: decimal(line.match(/([\d.,]+) ms/)[1]) });
      }
    }
  }
  if (records.length !== 20) throw new Error(`Expected 20 records, got ${records.length}`);
  for (const r of records) {
    for (const value of Object.values(r)) {
      if (typeof value === 'number' && !Number.isFinite(value)) throw new Error(`Invalid metric: ${JSON.stringify(r)}`);
    }
  }
  return records;
}
function report(records) {
  fs.writeFileSync(path.join(out, 'results.json'), JSON.stringify(records, null, 2));
  const baseline = git('rev-parse', 'HEAD').toString().trim();
  const lines = ['# Protocol performance comparison', '',
    `Baseline: \`${baseline}\` (original SyncNodes files); after: current working-tree protocol.`, '',
    'Release / .NET 9, three runs per version, alternating order, test-class parallelism disabled. '
      + 'Both isolated copies use identical SHA-256(counter) inputs for the existing performance tests; the bits sweep already uses seeded SHA-256 keys. '
      + 'Local times are medians of sync-only stopwatch measurements (including lazy preparation where the test includes it), excluding setup and test-runner time. '
      + 'No separate warm-up; small timings are noisy. All 12 performance tests passed on every run.', '',
    'Latency is modeled at 50 ms RTT and 100 Mbps, not measured networking. Bytes are Tx + Rx payload, excluding framing. '
      + 'Existing deletion scenarios choose contiguous sorted keys.', '',
    '| Scenario | Bits | Trips before → after | Bytes before → after | Byte change | Estimated ms before → after | Local ms before → after |',
    '|---|---:|---:|---:|---:|---:|---:|'];
  const keys = [...new Set(records.map(r => JSON.stringify([r.scenario, r.bits])))].sort();
  for (const key of keys) {
    const [name, bits] = JSON.parse(key);
    const groups = ['before', 'after'].map(v => records.filter(r => r.scenario === name && r.bits === bits && r.version === v));
    for (const g of groups) {
      if (g.length !== 3 || new Set(g.map(r => JSON.stringify([r.trips, r.rx, r.tx]))).size !== 1) throw new Error(`Inconsistent results: ${key}`);
    }
    const [b, a] = groups.map(g => g[0]);
    const times = groups.map(g => g[0].elapsed_ms === null ? null : g.map(r => r.elapsed_ms).sort((a,b) => a-b)[1]);
    const local = times[0] === null ? '—' : `${times[0].toFixed(2)} → ${times[1].toFixed(2)}`;
    if (b.rx === null) { lines.push(`| ${name} | — | — | — | — | — | ${local} |`); continue; }
    const bb = b.rx + b.tx, ab = a.rx + a.tx;
    lines.push(`| ${name} | ${bits} | ${b.trips} → ${a.trips} | ${bb.toLocaleString('en-US')} → ${ab.toLocaleString('en-US')} | ${((ab / bb - 1) * 100).toFixed(1)}% | ${b.latency_ms.toFixed(2)} → ${a.latency_ms.toFixed(2)} | ${local} |`);
  }
  fs.writeFileSync(path.join(out, 'comparison.md'), lines.join('\n') + '\n');
}

const reportOnly = process.argv.includes('--report-only');
if (!reportOnly) console.log('Preparing isolated Release builds');
const projects = reportOnly ? {} : Object.fromEntries(['before', 'after'].map(v => [v, prepare(v)]));
const records = [];
for (let run = 1; run <= 3; run++) {
  for (const version of (run % 2 ? ['before', 'after'] : ['after', 'before'])) {
    console.log(`Running ${version}, repetition ${run}/3`);
    const trx = path.join(out, `${version}-${run}.trx`);
    if (!reportOnly) command(['test', projects[version], '-c', 'Release', '--no-build', '--no-restore', '--filter', filter,
      '--logger', `trx;LogFileName=${trx}`, '--logger', 'console;verbosity=normal'], `${version}-${run}.log`);
    records.push(...parse(trx, version, run));
    fs.writeFileSync(path.join(out, 'results.partial.json'), JSON.stringify(records, null, 2));
    console.log(`Passed ${version}, repetition ${run}/3`);
  }
}
report(records);
console.log(`Report: ${path.join(out, 'comparison.md')}`);
