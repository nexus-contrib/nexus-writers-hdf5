using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using PureHDF;
using PureHDF.Filters;
using PureHDF.Selections;
using PureHDF.VOL.Native;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace Nexus.Writers
{
    [DataWriterDescription(DESCRIPTION)]
    [ExtensionDescription(
        "Store data in the HDF5 1.10 file format.",
        "https://github.com/Apollo3zehn/nexus-writers-hdf5",
        "https://github.com/Apollo3zehn/nexus-writers-hdf5")]
    public class Hdf5 : IDataWriter
    {
        #region "Fields"

        private const string DESCRIPTION = @"
{
  ""label"": ""HDF5 1.10 (*.h5)""
}
        ";

        private H5NativeWriter? _writer;
        private TimeSpan _lastSamplePeriod;
        private static readonly JsonSerializerOptions _serializerOptions;

        #endregion

        #region Constructors

        static Hdf5()
        {
            _serializerOptions = new JsonSerializerOptions()
            {
                WriteIndented = true
            };
        }

        #endregion

        #region Properties

        private DataWriterContext Context { get; set; } = default!;

        #endregion

        #region "Methods"

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
                var filePath = Path.Combine(root, $"{fileBegin.ToString("yyyy-MM-ddTHH-mm-ss")}Z_{samplePeriod.ToUnitString()}.h5");

                if (File.Exists(filePath))
                    throw new Exception($"The file {filePath} already exists. Extending an already existing file with additional resources is not supported.");

                var h5File = new H5File();

                // file
                h5File.Attributes["date_time"] = fileBegin.ToString("yyyy-MM-ddTHH-mm-ssZ");
                h5File.Attributes["sample_period"] = samplePeriod.ToUnitString();

                foreach (var catalogItemGroup in catalogItems.GroupBy(catalogItem => catalogItem.Catalog))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // file -> catalog
                    var catalog = catalogItemGroup.Key;
                    var physicalId = catalog.Id.TrimStart('/').Replace('/', '_');
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
                            (var chunkLength, var chunkCount) = GeneralHelper.CalculateChunkParameters(totalLength);
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
                if (_writer is null)
                    return;

                var offset = (ulong)(fileOffset.Ticks / _lastSamplePeriod.Ticks);

                var requestGroups = requests
                    .GroupBy(request => request.CatalogItem.Catalog)
                    .ToList();

                var processed = 0;

                foreach (var requestGroup in requestGroups)
                {
                    var catalog = requestGroup.Key;
                    var physicalId = catalog.Id.TrimStart('/').Replace('/', '_');
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

        private unsafe void WriteData(H5NativeWriter writer, string catalogPhysicalId, ulong fileOffset, WriteRequest writeRequest)
        {
            var length = (ulong)writeRequest.Data.Length;
            var catalogGroup = (H5Group)writer.File[catalogPhysicalId];
            var resourceGroup = (H5Group)catalogGroup[writeRequest.CatalogItem.Resource.Id];
            var datasetName = $"dataset_{writeRequest.CatalogItem.Representation.Id}{GetRepresentationParameterString(writeRequest.CatalogItem.Parameters)}";
            var dataset = (H5Dataset<Memory<double>>)resourceGroup[datasetName];
            var hyperslab = new HyperslabSelection(fileOffset, length);

            writer.Write(
                dataset: dataset,
                data: MemoryMarshal.AsMemory(writeRequest.Data) /* PureHDF does not yet support ReadOnlyMemory (v1.0.0-beta.2) */,
                fileSelection: hyperslab);
        }

        private static void PrepareResource(H5Group resourceGroup, CatalogItem catalogItem, uint chunkLength, ulong chunkCount)
        {
            if (chunkLength <= 0)
                throw new Exception(ErrorMessage.Hdf5Writer_SampleRateTooLow);

            // file -> catalog -> resource -> properties
            if (catalogItem.Resource.Properties is not null)
            {
                var key = "properties";
                var value = JsonSerializer.Serialize(catalogItem.Resource.Properties, _serializerOptions);

                resourceGroup.Attributes[key] = value;
            }

            // file -> catalog -> resource -> representation
            var datasetCreation = new H5DatasetCreation(
                Filters: new()
                {
                    ShuffleFilter.Id,
                    DeflateFilter.Id
                }
            );

            var representation = new H5Dataset<Memory<double>>(
                fileDims: new ulong[] { chunkLength * chunkCount },
                chunks: new uint[] { chunkLength },
                datasetCreation: datasetCreation );

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

        #endregion
    }
}