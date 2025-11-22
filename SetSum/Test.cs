using System.Security.Cryptography;
using Xunit;

namespace Setsum;

using static SetsumHashing;

public class SetsumTests
{
    private static readonly byte[] Data1Bytes = Convert.FromHexString("982051FD1E4BA744BBBE680E1FEE14677BA1A3C3540BF7B1CDB606E857233E0E00000000010000000100F2052A0100000043410496B538E853519C726A2C91E61EC11600AE1390813A627C66FB8BE7947BE63C52DA7589379515D4E0A604F8141781E62294721166BF621E73A82CBF2342C858EEAC");
    private static readonly byte[] Data2Bytes = Convert.FromHexString("D5FDCC541E25DE1C7A5ADDEDF24858B8BB665C9F36EF744EE42C316022C90F9B00000000020000000100F2052A010000004341047211A824F55B505228E4C3D5194C1FCFAA15A456ABDF37F9B9D97A4040AFC073DEE6C89064984F03385237D92167C13E236446B417AB79A0FCAE412AE3316B77AC");
    private static readonly byte[] Data3Bytes = Convert.FromHexString("44F672226090D85DB9A9F2FBFE5F0F9609B387AF7BE5B7FBB7A1767C831C9E9900000000030000000100F2052A0100000043410494B9D3E76C5B1629ECF97FFF95D7A4BBDAC87CC26099ADA28066C6FF1EB9191223CD897194A08D0C2726C5747F1DB49E8CF90E75DC3E3550AE9B30086F3CD5AAAC");

    private const string EmptyHash = "0000000000000000000000000000000000000000000000000000000000000000";
    private const string Data1Hash = "d63b1784e706370a88f0e110526a18f7bb567f50763856f4b19f094cfb89dd45";
    private const string Data2Hash = "626f788a3ecd1f0e966bc4bb37ca7b74424f98687a748bdcfa7cd51bc926f3b4";
    private const string Data3Hash = "dcdc8ef900b933854f7302a92e3d8ce05eff332425c736d9169b014cca31bc79";
    private const string Data1AndData2Hash = "3dab8f0e25d456181e5ca6ccec34946bfda517b977ade1d0ab1cdf67c4b0d0fa";
    private const string AllDataHash = "1e881e08258d8a9daecfa8757d72204c5ba54bdd237518aac1b7e0b347e38c74";

    private static byte[] HashBytes(byte[] bytes)
    {
        return SHA256.HashData(bytes);
    }

    [Fact]
    public void Empty()
    {
        var mset = new Setsum();
        Assert.Equal(EmptyHash, mset.GetHash());
    }

    [Fact]
    public void EmptyAdd()
    {
        var mset = new Setsum();
        var mset2 = new Setsum();

        mset = mset + mset2;

        Assert.Equal(EmptyHash, mset.GetHash());
    }

    [Fact]
    public void Data1ByHash()
    {
        var mset = new Setsum();
        mset = mset.InsertHash(HashBytes(Data1Bytes));

        Assert.Equal(Data1Hash, mset.GetHash());
    }

    [Fact]
    public void Data1()
    {
        var mset = SHA256Setsum(Data1Bytes);

        Assert.Equal(Data1Hash, mset.GetHash());
    }

    [Fact]
    public void Data2()
    {
        var mset = SHA256Setsum(Data2Bytes);

        Assert.Equal(Data2Hash, mset.GetHash());
    }

    [Fact]
    public void Data3()
    {
        var mset = SHA256Setsum(Data3Bytes);

        Assert.Equal(Data3Hash, mset.GetHash());
    }

    [Fact]
    public void Data1MultisetPlusData2Multiset()
    {
        var mset12 = SHA256Setsum(Data1Bytes) + SHA256Setsum(Data2Bytes);

        Assert.Equal(Data1AndData2Hash, mset12.GetHash());
    }

    [Fact]
    public void Data1MultisetPlusData2MultisetPlusData3Multiset()
    {
        var allHash = SHA256Setsum(Data1Bytes) + SHA256Setsum(Data2Bytes) + SHA256Setsum(Data3Bytes);

        Assert.Equal(AllDataHash, allHash.GetHash());
    }

    [Fact]
    public void Data1PlusData2PlusData3MinusData3()
    {
        var mset = SHA256Setsum(Data1Bytes) + SHA256Setsum(Data2Bytes) + SHA256Setsum(Data3Bytes);
        mset = RemoveSHA256(mset, Data3Bytes);

        Assert.Equal(Data1AndData2Hash, mset.GetHash());
    }

    [Fact]
    public void Data1PlusData2PlusData3MultisetMinusData2PlusData3Multiset()
    {
        var mset = SHA256Setsum(Data1Bytes) + SHA256Setsum(Data2Bytes) + SHA256Setsum(Data3Bytes);
        var mset2 = SHA256Setsum(Data2Bytes) + SHA256Setsum(Data3Bytes);

        mset = mset - mset2;

        Assert.Equal(Data1Hash, mset.GetHash());
    }

    [Fact]
    public void Data2PlusData1PlusData3OrderDoesNotMatter()
    {
        var mset = SHA256Setsum(Data2Bytes) + SHA256Setsum(Data1Bytes) + SHA256Setsum(Data3Bytes);

        Assert.Equal(AllDataHash, mset.GetHash());
    }

    [Fact]
    public void MissingElement()
    {
        var msetAll = new Setsum();
        msetAll = msetAll.InsertHash(Convert.FromHexString(AllDataHash));

        var mset12 = new Setsum();
        mset12 = mset12.InsertHash(Convert.FromHexString(Data1AndData2Hash));

        var mset3 = msetAll - mset12;
        Assert.Equal(Data3Hash, mset3.GetHash());
    }

    [Fact]
    public void XXHash()
    {
        var mset = XxHash3Setsum(Data1Bytes);
        Assert.Equal("0ee0fe32faa2f177c4bcf0dbc508891cca5c0ee93faa786bd29cef0ec0ab7a94", mset.GetHash());

        var msetEmpty = RemoveXxHash3(mset, Data1Bytes);
        Assert.Equal(EmptyHash, msetEmpty.GetHash());
    }
}