using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using PureHDF;
using PureHDF.Filters;
using PureHDF.Selections;
using PureHDF.VOL.Native;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace Nexus.Writers;

[DataWriterDescription(DESCRIPTION)]
[ExtensionDescription(
    "Store data in the HDF5 1.10 file format.",
    "https://github.com/Apollo3zehn/nexus-writers-hdf5",
    "https://github.com/Apollo3zehn/nexus-writers-hdf5")]
public class Hdf5 : IDataWriter
{
    private const string DESCRIPTION = """
    {
        "label": "HDF5 1.10 (*.h5)"
    }
    """;

    private H5NativeWriter _writer = default!;
    private TimeSpan _lastSamplePeriod;
    private static readonly JsonSerializerOptions _serializerOptions;

    static Hdf5()
    {
        _serializerOptions = new JsonSerializerOptions()
        {
            WriteIndented = true
        };
    }

    private DataWriterContext Context { get; set; } = default!;

    public Task SetContextAsync(
       DataWriterContext context,
       ILogger logger,
       CancellationToken cancellationToken)
    {
        Context = context;
        return Task.CompletedTask;
    }

    public Task OpenAsync(
        DateTime fileBegin,
        TimeSpan filePeriod,
        TimeSpan samplePeriod,
        CatalogItem[] catalogItems,
        CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            _lastSamplePeriod = samplePeriod;

            var totalLength = (ulong)(filePeriod.Ticks / samplePeriod.Ticks);
            var root = Context.ResourceLocator.ToPath();
            var filePath = Path.Combine(root, $"{fileBegin:yyyy-MM-ddTHH-mm-ss}Z_{samplePeriod.ToUnitString()}.h5");

            if (File.Exists(filePath))
                throw new Exception($"The file {filePath} already exists. Extending an already existing file with additional resources is not supported.");

            var h5File = new H5File();

            // file
            h5File.Attributes["date_time"] = fileBegin.ToString("yyyy-MM-ddTHH-mm-ssZ");
            h5File.Attributes["sample_period"] = samplePeriod.ToUnitString();

            foreach (var catalogItemGroup in catalogItems.GroupBy(catalogItem => catalogItem.Catalog.Id))
            {
                cancellationToken.ThrowIfCancellationRequested();

                // file -> catalog
                var catalogId = catalogItemGroup.Key;
                var physicalId = catalogId.TrimStart('/').Replace('/', '_');
                var catalog = catalogItemGroup.First().Catalog;
                var catalogGroup = new H5Group();

                // file -> catalog -> properties
                if (catalog.Properties is not null)
                {
                    var key = "properties";
                    var value = JsonSerializer.Serialize(catalog.Properties, _serializerOptions);

                    catalogGroup.Attributes[key] = value;
                }

                // file -> catalog -> resources
                foreach (var groupedByResourceId in catalogItemGroup.GroupBy(catalogItem => catalogItem.Resource.Id))
                {
                    var resourceGroup = new H5Group();

                    foreach (var catalogItem in groupedByResourceId)
                    {
                        (var chunkLength, var chunkCount) = Utils.CalculateChunkParameters(totalLength);
                        PrepareResource(resourceGroup, catalogItem, chunkLength, chunkCount);
                    }

                    catalogGroup[groupedByResourceId.Key] = resourceGroup;
                }

                h5File[physicalId] = catalogGroup;
            }

            _writer = h5File.BeginWrite(filePath);
        }, cancellationToken);
    }

    public Task WriteAsync(
        TimeSpan fileOffset,
        WriteRequest[] requests,
        IProgress<double> progress,
        CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            var offset = (ulong)(fileOffset.Ticks / _lastSamplePeriod.Ticks);

            var requestGroups = requests
                .GroupBy(request => request.CatalogItem.Catalog.Id)
                .ToList();

            var processed = 0;

            foreach (var requestGroup in requestGroups)
            {
                var catalogId = requestGroup.Key;
                var physicalId = catalogId.TrimStart('/').Replace('/', '_');
                var writeRequests = requestGroup.ToArray();

                for (int i = 0; i < writeRequests.Length; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    WriteData(_writer, physicalId, offset, writeRequests[i]);
                }

                processed++;
                progress.Report((double)processed / requests.Length);
            }
        }, cancellationToken);
    }

    public Task CloseAsync(CancellationToken cancellationToken)
    {
        _writer?.Dispose();

        return Task.CompletedTask;
    }

    private static void WriteData(H5NativeWriter writer, string physicalCatalogId, ulong fileOffset, WriteRequest writeRequest)
    {
        var length = (ulong)writeRequest.Data.Length;
        var catalogGroup = (H5Group)writer.File[physicalCatalogId];
        var resourceGroup = (H5Group)catalogGroup[writeRequest.CatalogItem.Resource.Id];
        var datasetName = $"dataset_{writeRequest.CatalogItem.Representation.Id}{GetRepresentationParameterString(writeRequest.CatalogItem.Parameters)}";
        var dataset = (H5Dataset<Memory<double>>)resourceGroup[datasetName];
        var selection = new HyperslabSelection(fileOffset, length);

        writer.Write(
            dataset: dataset,
            data: MemoryMarshal.AsMemory(writeRequest.Data) /* PureHDF does not yet support ReadOnlyMemory (v2.1.1) */,
            fileSelection: selection);
    }

    private static void PrepareResource(H5Group resourceGroup, CatalogItem catalogItem, uint chunkLength, ulong chunkCount)
    {
        if (chunkLength <= 0)
            throw new Exception("The sample rate is too low.");

        // file -> catalog -> resource -> properties
        if (catalogItem.Resource.Properties is not null)
        {
            var key = "properties";
            var value = JsonSerializer.Serialize(catalogItem.Resource.Properties, _serializerOptions);

            resourceGroup.Attributes[key] = value;
        }

        // file -> catalog -> resource -> representation
        var datasetCreation = new H5DatasetCreation(
            Filters:
            [
                ShuffleFilter.Id,
                DeflateFilter.Id
            ]
        );

        var representation = new H5Dataset<Memory<double>>(
            fileDims: [chunkLength * chunkCount],
            chunks: [chunkLength],
            datasetCreation: datasetCreation);

        var datasetName = $"dataset_{catalogItem.Representation.Id}{GetRepresentationParameterString(catalogItem.Parameters)}";

        resourceGroup[datasetName] = representation;
    }

    private static string? GetRepresentationParameterString(IReadOnlyDictionary<string, string>? parameters)
    {
        if (parameters is null)
            return default;

        var serializedParameters = parameters
            .Select(parameter => $"{parameter.Key}={parameter.Value}");

        var parametersString = $"({string.Join(',', serializedParameters)})";

        return parametersString;
    }
}