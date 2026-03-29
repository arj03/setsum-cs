import { readFileSync } from "fs";
import { createHash } from "crypto";

const wasm = readFileSync(new URL("./build/setsum.wasm", import.meta.url));
const { instance } = await WebAssembly.instantiate(wasm, {
  env: { abort: () => { throw new Error("abort"); } }
});

const {
  memory,
  setsum_empty, setsum_is_empty, setsum_equals,
  setsum_hash, setsum_insert, setsum_remove,
  setsum_add, setsum_sub,
  setsum_insert_batch, setsum_remove_batch
} = instance.exports;

const mem = () => new Uint8Array(memory.buffer);

// Scratch pointers
const STATE = 2048;
const HASH  = 2048 + 32;
const OUT   = 2048 + 64;
const OUT2  = 2048 + 96;
const TMP   = 2048 + 128;

// --- Helpers ----------------------------------------------------------------

function hexToBytes(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
  return bytes;
}

function readHex(ptr) {
  return Array.from(mem().slice(ptr, ptr + 32)).map(b => b.toString(16).padStart(2, "0")).join("");
}

function sha256(data) {
  return createHash("sha256").update(data).digest();
}

/** Compute SHA256 of data, write hash to WASM at hashPtr, insert into statePtr, write result to outPtr. */
function sha256Insert(statePtr, data, outPtr) {
  const h = sha256(data);
  mem().set(h, HASH);
  setsum_insert(statePtr, HASH, outPtr);
}

/** SHA256Setsum: empty + insert(SHA256(data)) */
function sha256Setsum(data, outPtr) {
  setsum_empty(outPtr);
  sha256Insert(outPtr, data, outPtr);
}

/** RemoveSHA256: state - SHA256(data) */
function sha256Remove(statePtr, data, outPtr) {
  const h = sha256(data);
  mem().set(h, HASH);
  setsum_remove(statePtr, HASH, outPtr);
}

// --- Test data (matching C# Test.cs) ----------------------------------------

const Data1Bytes = hexToBytes("982051FD1E4BA744BBBE680E1FEE14677BA1A3C3540BF7B1CDB606E857233E0E00000000010000000100F2052A0100000043410496B538E853519C726A2C91E61EC11600AE1390813A627C66FB8BE7947BE63C52DA7589379515D4E0A604F8141781E62294721166BF621E73A82CBF2342C858EEAC");
const Data2Bytes = hexToBytes("D5FDCC541E25DE1C7A5ADDEDF24858B8BB665C9F36EF744EE42C316022C90F9B00000000020000000100F2052A010000004341047211A824F55B505228E4C3D5194C1FCFAA15A456ABDF37F9B9D97A4040AFC073DEE6C89064984F03385237D92167C13E236446B417AB79A0FCAE412AE3316B77AC");
const Data3Bytes = hexToBytes("44F672226090D85DB9A9F2FBFE5F0F9609B387AF7BE5B7FBB7A1767C831C9E9900000000030000000100F2052A0100000043410494B9D3E76C5B1629ECF97FFF95D7A4BBDAC87CC26099ADA28066C6FF1EB9191223CD897194A08D0C2726C5747F1DB49E8CF90E75DC3E3550AE9B30086F3CD5AAAC");

const EmptyHash        = "0000000000000000000000000000000000000000000000000000000000000000";
const Data1Hash        = "d63b1784e706370a88f0e110526a18f7bb567f50763856f4b19f094cfb89dd45";
const Data2Hash        = "626f788a3ecd1f0e966bc4bb37ca7b74424f98687a748bdcfa7cd51bc926f3b4";
const Data3Hash        = "dcdc8ef900b933854f7302a92e3d8ce05eff332425c736d9169b014cca31bc79";
const Data1AndData2Hash = "3dab8f0e25d456181e5ca6ccec34946bfda517b977ade1d0ab1cdf67c4b0d0fa";
const AllDataHash      = "1e881e08258d8a9daecfa8757d72204c5ba54bdd237518aac1b7e0b347e38c74";

let passed = 0;
let failed = 0;

function assert(condition, name) {
  if (condition) {
    console.log(`  PASS: ${name}`);
    passed++;
  } else {
    console.error(`  FAIL: ${name}`);
    failed++;
  }
}

function assertHash(ptr, expected, name) {
  const actual = readHex(ptr);
  if (actual === expected) {
    console.log(`  PASS: ${name}`);
    passed++;
  } else {
    console.error(`  FAIL: ${name}`);
    console.error(`    expected: ${expected}`);
    console.error(`    actual:   ${actual}`);
    failed++;
  }
}

// --- Tests (mirroring C# SetsumTests) ---------------------------------------

console.log("SetsumTests:");

// Empty
setsum_empty(STATE);
assertHash(STATE, EmptyHash, "Empty");

// EmptyAdd
setsum_empty(STATE);
setsum_empty(OUT);
setsum_add(STATE, OUT, OUT2);
assertHash(OUT2, EmptyHash, "EmptyAdd");

// Data1ByHash
setsum_empty(STATE);
mem().set(sha256(Data1Bytes), HASH);
setsum_insert(STATE, HASH, OUT);
assertHash(OUT, Data1Hash, "Data1ByHash");

// Data1
sha256Setsum(Data1Bytes, STATE);
assertHash(STATE, Data1Hash, "Data1");

// Data2
sha256Setsum(Data2Bytes, STATE);
assertHash(STATE, Data2Hash, "Data2");

// Data3
sha256Setsum(Data3Bytes, STATE);
assertHash(STATE, Data3Hash, "Data3");

// Data1MultisetPlusData2Multiset
sha256Setsum(Data1Bytes, STATE);
sha256Setsum(Data2Bytes, OUT);
setsum_add(STATE, OUT, OUT2);
assertHash(OUT2, Data1AndData2Hash, "Data1MultisetPlusData2Multiset");

// Data1MultisetPlusData2MultisetPlusData3Multiset
sha256Setsum(Data1Bytes, STATE);
sha256Setsum(Data2Bytes, OUT);
setsum_add(STATE, OUT, OUT2);        // OUT2 = {1} + {2}
sha256Setsum(Data3Bytes, STATE);
setsum_add(OUT2, STATE, OUT);        // OUT = {1} + {2} + {3}
assertHash(OUT, AllDataHash, "Data1MultisetPlusData2MultisetPlusData3Multiset");

// Data1PlusData2PlusData3MinusData3
sha256Setsum(Data1Bytes, STATE);
sha256Setsum(Data2Bytes, OUT);
setsum_add(STATE, OUT, OUT2);        // OUT2 = {1} + {2}
sha256Setsum(Data3Bytes, STATE);
setsum_add(OUT2, STATE, OUT);        // OUT = {1,2,3}
sha256Remove(OUT, Data3Bytes, OUT2); // OUT2 = {1,2,3} - {3}
assertHash(OUT2, Data1AndData2Hash, "Data1PlusData2PlusData3MinusData3");

// Data1PlusData2PlusData3MultisetMinusData2PlusData3Multiset
sha256Setsum(Data1Bytes, STATE);
sha256Setsum(Data2Bytes, OUT);
setsum_add(STATE, OUT, OUT2);        // OUT2 = {1} + {2}
sha256Setsum(Data3Bytes, STATE);
setsum_add(OUT2, STATE, OUT);        // OUT = {1,2,3}
sha256Setsum(Data2Bytes, OUT2);
sha256Setsum(Data3Bytes, STATE);
setsum_add(OUT2, STATE, TMP);        // TMP = {2} + {3}
setsum_sub(OUT, TMP, STATE);         // STATE = {1,2,3} - {2,3}
assertHash(STATE, Data1Hash, "Data1PlusData2PlusData3MultisetMinusData2PlusData3Multiset");

// Data2PlusData1PlusData3OrderDoesNotMatter
sha256Setsum(Data2Bytes, STATE);
sha256Setsum(Data1Bytes, OUT);
setsum_add(STATE, OUT, OUT2);        // OUT2 = {2} + {1}
sha256Setsum(Data3Bytes, STATE);
setsum_add(OUT2, STATE, OUT);        // OUT = {2,1,3}
assertHash(OUT, AllDataHash, "Data2PlusData1PlusData3OrderDoesNotMatter");

// MissingElement: allHash as raw insert, subtract data1And2Hash as raw insert, get data3Hash
setsum_empty(STATE);
mem().set(hexToBytes(AllDataHash), HASH);
setsum_insert(STATE, HASH, OUT);     // OUT = insert(allHash)
setsum_empty(STATE);
mem().set(hexToBytes(Data1AndData2Hash), HASH);
setsum_insert(STATE, HASH, OUT2);    // OUT2 = insert(data1And2Hash)
setsum_sub(OUT, OUT2, STATE);        // STATE = allHash - data1And2Hash
assertHash(STATE, Data3Hash, "MissingElement");

// --- Batch tests ------------------------------------------------------------

console.log("\nBatch tests:");

// Batch insert matches sequential
const BATCH_BASE = 4096;
const h1 = sha256(Data1Bytes);
const h2 = sha256(Data2Bytes);
const h3 = sha256(Data3Bytes);
mem().set(h1, BATCH_BASE);
mem().set(h2, BATCH_BASE + 32);
mem().set(h3, BATCH_BASE + 64);

setsum_empty(STATE);
setsum_insert_batch(STATE, BATCH_BASE, 3);
assertHash(STATE, AllDataHash, "BatchInsertMatchesSequential");

// Batch remove reverses batch insert
setsum_remove_batch(STATE, BATCH_BASE, 3);
assertHash(STATE, EmptyHash, "BatchRemoveReversesInsert");

// Batch partial remove
setsum_empty(STATE);
setsum_insert_batch(STATE, BATCH_BASE, 3);  // {1,2,3}
setsum_remove_batch(STATE, BATCH_BASE + 64, 1); // remove {3}
assertHash(STATE, Data1AndData2Hash, "BatchPartialRemove");

// --- Summary ----------------------------------------------------------------

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
