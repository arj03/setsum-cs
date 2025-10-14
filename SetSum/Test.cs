using System.Security.Cryptography;
using Xunit;

namespace Setsum;

public class SetsumTests
{
    private static readonly byte[] Data1Bytes = Convert.FromHexString("982051FD1E4BA744BBBE680E1FEE14677BA1A3C3540BF7B1CDB606E857233E0E00000000010000000100F2052A0100000043410496B538E853519C726A2C91E61EC11600AE1390813A627C66FB8BE7947BE63C52DA7589379515D4E0A604F8141781E62294721166BF621E73A82CBF2342C858EEAC");
    private static readonly byte[] Data2Bytes = Convert.FromHexString("D5FDCC541E25DE1C7A5ADDEDF24858B8BB665C9F36EF744EE42C316022C90F9B00000000020000000100F2052A010000004341047211A824F55B505228E4C3D5194C1FCFAA15A456ABDF37F9B9D97A4040AFC073DEE6C89064984F03385237D92167C13E236446B417AB79A0FCAE412AE3316B77AC");
    private static readonly byte[] Data3Bytes = Convert.FromHexString("44F672226090D85DB9A9F2FBFE5F0F9609B387AF7BE5B7FBB7A1767C831C9E9900000000030000000100F2052A0100000043410494B9D3E76C5B1629ECF97FFF95D7A4BBDAC87CC26099ADA28066C6FF1EB9191223CD897194A08D0C2726C5747F1DB49E8CF90E75DC3E3550AE9B30086F3CD5AAAC");

    private const string EmptyHash = "0000000000000000000000000000000000000000000000000000000000000000";
    private const string Data1Hash = "f6173737e77f789f26fb288f6e0ce67703de5e45b6250f29728d38c69ba1196a";
    private const string Data2Hash = "0205362fc6e33bcd13576e5a58afc80637275731c307a8007cab765dc1a706f2";
    private const string Data3Hash = "bcac6eb1f17a463997c4ce7aeaee0b6d04ed8a903cead0adc48f7e3a52919bf5";
    private const string Data1AndData2Hash = "f81c6d66be63b46c395297e9c6bbae7e3a05b676792db7298739af23154a205c";
    private const string AllDataHash = "b9c9db17afdefaa511176664b0aabaeba9f24007b51788d74bc92d5e20dcbb51";

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
    public void Data1()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(Data1Bytes));

        Assert.Equal(Data1Hash, mset.GetHash());
    }

    [Fact]
    public void Data2()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(Data2Bytes));

        Assert.Equal(Data2Hash, mset.GetHash());
    }

    [Fact]
    public void Data3()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(Data3Bytes));

        Assert.Equal(Data3Hash, mset.GetHash());
    }

    [Fact]
    public void Data1MultisetPlusData2Multiset()
    {
        var mset1 = new Setsum();
        mset1 = mset1.Insert(HashBytes(Data1Bytes));

        var mset2 = new Setsum();
        mset2 = mset2.Insert(HashBytes(Data2Bytes));

        mset1 = mset1 + mset2;

        Assert.Equal(Data1AndData2Hash, mset1.GetHash());
    }

    [Fact]
    public void Data1PlusData2()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(Data1Bytes));
        mset = mset.Insert(HashBytes(Data2Bytes));

        Assert.Equal(Data1AndData2Hash, mset.GetHash());
    }

    [Fact]
    public void Data1MultisetPlusData2MultisetPlusData3Multiset()
    {
        var mset1 = new Setsum();
        mset1 = mset1.Insert(HashBytes(Data1Bytes));

        var mset2 = new Setsum();
        mset2 = mset2.Insert(HashBytes(Data2Bytes));

        var mset3 = new Setsum();
        mset3 = mset3.Insert(HashBytes(Data3Bytes));

        mset1 = mset1 + mset2 + mset3;

        Assert.Equal(AllDataHash, mset1.GetHash());
    }

    [Fact]
    public void Data1PlusData2PlusData3()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(Data1Bytes));
        mset = mset.Insert(HashBytes(Data2Bytes));
        mset = mset.Insert(HashBytes(Data3Bytes));

        Assert.Equal(AllDataHash, mset.GetHash());
    }

    [Fact]
    public void Data1PlusData2PlusData3MinusData3()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(Data1Bytes));
        mset = mset.Insert(HashBytes(Data2Bytes));
        mset = mset.Insert(HashBytes(Data3Bytes));
        mset = mset.Remove(HashBytes(Data3Bytes));

        Assert.Equal(Data1AndData2Hash, mset.GetHash());
    }

    [Fact]
    public void Data1PlusData2PlusData3MultisetMinusData2PlusData3Multiset()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(Data1Bytes));
        mset = mset.Insert(HashBytes(Data2Bytes));
        mset = mset.Insert(HashBytes(Data3Bytes));

        var mset2 = new Setsum();
        mset2 = mset2.Insert(HashBytes(Data2Bytes));
        mset2 = mset2.Insert(HashBytes(Data3Bytes));

        mset = mset - mset2;

        Assert.Equal(Data1Hash, mset.GetHash());
    }

    [Fact]
    public void Data2PlusData1PlusData3OrderDoesNotMatter()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(Data2Bytes));
        mset = mset.Insert(HashBytes(Data1Bytes));
        mset = mset.Insert(HashBytes(Data3Bytes));

        Assert.Equal(AllDataHash, mset.GetHash());
    }
}