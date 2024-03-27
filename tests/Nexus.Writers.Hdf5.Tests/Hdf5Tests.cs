using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using PureHDF;
using Xunit;

namespace Nexus.Writers.Tests;

public class Hdf5Tests(DataWriterFixture fixture) : IClassFixture<DataWriterFixture>
{
    private readonly DataWriterFixture _fixture = fixture;

    [Fact]
    public async Task CanWriteFiles()
    {
        // TODO replace this test when PureHDF offers a global H5Dump tool

        var options = new JsonSerializerOptions() { WriteIndented = true };
        var targetFolder = _fixture.GetTargetFolder();
        var dataWriter = new Hdf5() as IDataWriter;

        var context = new DataWriterContext(
            ResourceLocator: new Uri(targetFolder),
            SystemConfiguration: default!,
            RequestConfiguration: default!);

        await dataWriter.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var begin = new DateTime(2020, 01, 01, 0, 0, 0, DateTimeKind.Utc);
        var samplePeriod = TimeSpan.FromSeconds(1);

        var catalogItems = _fixture.Catalogs.SelectMany(catalog => catalog.Resources!
            .SelectMany(resource => resource.Representations!.Select(representation => new CatalogItem(catalog, resource, representation, default))))
            .ToArray();

        var random = new Random(Seed: 1);

        var length = 1000;

        var data = new[]
        {
            Enumerable
                .Range(0, length)
                .Select(value => random.NextDouble() * 1e4)
                .ToArray(),

            Enumerable
                .Range(0, length)
                .Select(value => random.NextDouble() * -1)
                .ToArray(),

            Enumerable
                .Range(0, length)
                .Select(value => random.NextDouble() * Math.PI)
                .ToArray()
        };

        var requests = catalogItems
            .Select((catalogItem, i) => new WriteRequest(catalogItem, data[i]))
            .ToArray();

        await dataWriter.OpenAsync(begin, TimeSpan.FromSeconds(2000), samplePeriod, catalogItems, CancellationToken.None);
        await dataWriter.WriteAsync(TimeSpan.Zero, requests, new Progress<double>(), CancellationToken.None);
        await dataWriter.WriteAsync(TimeSpan.FromSeconds(length), requests, new Progress<double>(), CancellationToken.None);
        await dataWriter.CloseAsync(CancellationToken.None);

        var actualFilePaths = Directory
            .GetFiles(targetFolder)
            .OrderBy(value => value)
            .ToArray();

        // assert
        Assert.Single(actualFilePaths);

        using var h5File = H5File.OpenRead(actualFilePaths.First());

        Assert.Equal(2, h5File.Children().Count());

        // catalog 1
        var catalog1 = h5File.Group("A_B_C");
        var resources1 = _fixture.Catalogs[0].Resources!;
        Assert.Equal(resources1.Count, catalog1.Children().Count());
        var representations1 = _fixture.Catalogs[0].Resources!.SelectMany(resource => resource.Representations!).ToList();
        Assert.Equal(representations1.Count, catalog1.Group("resource1").Children().Count());

        Assert.Equal(
            JsonSerializer.Serialize(_fixture.Catalogs[0].Properties, options),
            catalog1.Attribute("properties").Read<string>());

        Assert.Equal(
            JsonSerializer.Serialize(_fixture.Catalogs[0].Resources![0].Properties, options),
            catalog1.Group("resource1").Attribute("properties").Read<string>());

        var actualData0 = catalog1
            .Group("resource1")
            .Dataset("dataset_1_s")
            .Read<double[]>();

        Assert.True(data[0].Concat(data[0]).SequenceEqual(actualData0));

        var actualData1 = catalog1
            .Group("resource1")
            .Dataset("dataset_2_s")
            .Read<double[]>();

        Assert.True(data[1].Concat(data[1]).SequenceEqual(actualData1));

        // catalog 2
        var catalog2 = h5File.Group("D_E_F");
        var resources2 = _fixture.Catalogs[0].Resources;
        Assert.Equal(resources2!.Count, catalog2.Children().Count());
        var representations2 = _fixture.Catalogs[1].Resources!.SelectMany(resource => resource.Representations!).ToList();
        Assert.Equal(representations2.Count, catalog2.Group("resource3").Children().Count());

        Assert.Equal(
            JsonSerializer.Serialize(_fixture.Catalogs[1].Properties, options),
            catalog2.Attribute("properties").Read<string>());

        Assert.Equal(
            JsonSerializer.Serialize(_fixture.Catalogs[1].Resources![0].Properties, options),
            catalog2.Group("resource3").Attribute("properties").Read<string>());

        var actualData2 = catalog2
            .Group("resource3")
            .Dataset("dataset_1_s")
            .Read<double[]>();

        Assert.True(data[2].Concat(data[2]).SequenceEqual(actualData2));
    }
}