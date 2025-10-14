using System.Security.Cryptography;
using Xunit;

namespace Setsum;

public class SetsumTests
{
    private static readonly byte[] D1_BYTES = Convert.FromHexString("982051FD1E4BA744BBBE680E1FEE14677BA1A3C3540BF7B1CDB606E857233E0E00000000010000000100F2052A0100000043410496B538E853519C726A2C91E61EC11600AE1390813A627C66FB8BE7947BE63C52DA7589379515D4E0A604F8141781E62294721166BF621E73A82CBF2342C858EEAC");
    private static readonly byte[] D2_BYTES = Convert.FromHexString("D5FDCC541E25DE1C7A5ADDEDF24858B8BB665C9F36EF744EE42C316022C90F9B00000000020000000100F2052A010000004341047211A824F55B505228E4C3D5194C1FCFAA15A456ABDF37F9B9D97A4040AFC073DEE6C89064984F03385237D92167C13E236446B417AB79A0FCAE412AE3316B77AC");
    private static readonly byte[] D3_BYTES = Convert.FromHexString("44F672226090D85DB9A9F2FBFE5F0F9609B387AF7BE5B7FBB7A1767C831C9E9900000000030000000100F2052A0100000043410494B9D3E76C5B1629ECF97FFF95D7A4BBDAC87CC26099ADA28066C6FF1EB9191223CD897194A08D0C2726C5747F1DB49E8CF90E75DC3E3550AE9B30086F3CD5AAAC");

    private static readonly string EMPTY_HASH = new('0', 64);

    private static byte[] HashBytes(byte[] bytes)
    {
        return SHA256.HashData(bytes);
    }

    [Fact]
    public void Empty()
    {
        var mset = new Setsum();
        Assert.Equal(EMPTY_HASH, mset.GetHash());
    }

    [Fact]
    public void EmptyAdd()
    {
        var mset = new Setsum();
        var mset2 = new Setsum();

        mset = mset + mset2;

        Assert.Equal(EMPTY_HASH, mset.GetHash());
    }

    [Fact]
    public void D1()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(D1_BYTES));

        const string expected = "f6173737e77f789f26fb288f6e0ce67703de5e45b6250f29728d38c69ba1196a";
        Assert.Equal(expected, mset.GetHash());
    }

    [Fact]
    public void D2()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(D2_BYTES));

        const string expected = "0205362fc6e33bcd13576e5a58afc80637275731c307a8007cab765dc1a706f2";
        Assert.Equal(expected, mset.GetHash());
    }

    [Fact]
    public void D3()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(D3_BYTES));

        const string expected = "bcac6eb1f17a463997c4ce7aeaee0b6d04ed8a903cead0adc48f7e3a52919bf5";
        Assert.Equal(expected, mset.GetHash());
    }

    [Fact]
    public void D1_MS_Plus_D2_MS()
    {
        var mset1 = new Setsum();
        mset1 = mset1.Insert(HashBytes(D1_BYTES));

        var mset2 = new Setsum();
        mset2 = mset2.Insert(HashBytes(D2_BYTES));

        mset1 = mset1 + mset2;

        const string expected = "f81c6d66be63b46c395297e9c6bbae7e3a05b676792db7298739af23154a205c";
        Assert.Equal(expected, mset1.GetHash());
    }

    [Fact]
    public void D1_Plus_D2()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(D1_BYTES));
        mset = mset.Insert(HashBytes(D2_BYTES));

        const string expected = "f81c6d66be63b46c395297e9c6bbae7e3a05b676792db7298739af23154a205c";
        Assert.Equal(expected, mset.GetHash());
    }

    [Fact]
    public void D1_MS_Plus_D2_MS_Plus_D3_MS()
    {
        var mset1 = new Setsum();
        mset1 = mset1.Insert(HashBytes(D1_BYTES));

        var mset2 = new Setsum();
        mset2 = mset2.Insert(HashBytes(D2_BYTES));

        var mset3 = new Setsum();
        mset3 = mset3.Insert(HashBytes(D3_BYTES));

        mset1 = mset1 + mset2 + mset3;

        const string expected = "b9c9db17afdefaa511176664b0aabaeba9f24007b51788d74bc92d5e20dcbb51";
        Assert.Equal(expected, mset1.GetHash());
    }

    [Fact]
    public void D1_Plus_D2_Plus_D3()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(D1_BYTES));
        mset = mset.Insert(HashBytes(D2_BYTES));
        mset = mset.Insert(HashBytes(D3_BYTES));

        const string expected = "b9c9db17afdefaa511176664b0aabaeba9f24007b51788d74bc92d5e20dcbb51";
        Assert.Equal(expected, mset.GetHash());
    }

    [Fact]
    public void D1_Plus_D2_Plus_D3_Minus_D3()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(D1_BYTES));
        mset = mset.Insert(HashBytes(D2_BYTES));
        mset = mset.Insert(HashBytes(D3_BYTES));
        mset = mset.Remove(HashBytes(D3_BYTES));

        const string expected = "f81c6d66be63b46c395297e9c6bbae7e3a05b676792db7298739af23154a205c";
        Assert.Equal(expected, mset.GetHash());
    }

    [Fact]
    public void D1_Plus_D2_Plus_D3_MS_Minus_D2_Plus_D3_MS()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(D1_BYTES));
        mset = mset.Insert(HashBytes(D2_BYTES));
        mset = mset.Insert(HashBytes(D3_BYTES));

        var mset2 = new Setsum();
        mset2 = mset2.Insert(HashBytes(D2_BYTES));
        mset2 = mset2.Insert(HashBytes(D3_BYTES));

        mset = mset - mset2;

        const string expected = "f6173737e77f789f26fb288f6e0ce67703de5e45b6250f29728d38c69ba1196a";
        Assert.Equal(expected, mset.GetHash());
    }

    [Fact]
    public void D2_Plus_D1_Plus_D3_Order_Does_Not_Matter()
    {
        var mset = new Setsum();
        mset = mset.Insert(HashBytes(D2_BYTES));
        mset = mset.Insert(HashBytes(D1_BYTES));
        mset = mset.Insert(HashBytes(D3_BYTES));

        const string expected = "b9c9db17afdefaa511176664b0aabaeba9f24007b51788d74bc92d5e20dcbb51";
        Assert.Equal(expected, mset.GetHash());
    }
}