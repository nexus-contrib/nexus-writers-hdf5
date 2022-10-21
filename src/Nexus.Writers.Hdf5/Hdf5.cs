using HDF.PInvoke;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
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

        private long _fileId = -1;
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

                try
                {
                    _fileId = H5F.create(filePath, H5F.ACC_TRUNC);

                    if (_fileId < 0)
                        throw new Exception($"{ErrorMessage.Hdf5Writer_CouldNotOpenOrCreateFile} File: {filePath}.");

                    // file
                    PrepareStringAttribute(_fileId, "date_time", fileBegin.ToString("yyyy-MM-ddTHH-mm-ssZ"));
                    PrepareStringAttribute(_fileId, "sample_period", samplePeriod.ToUnitString());

                    foreach (var catalogItemGroup in catalogItems.GroupBy(catalogItem => catalogItem.Catalog))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        // file -> catalog
                        var catalog = catalogItemGroup.Key;
                        var physicalId = catalog.Id.TrimStart('/').Replace('/', '_');

                        long groupId = -1;

                        try
                        {
                            groupId = IOHelper.OpenOrCreateGroup(_fileId, physicalId).GroupId;

                            // file -> catalog -> properties
                            if (catalog.Properties is not null)
                            {
                                var key = "properties";
                                var value = JsonSerializer.Serialize(catalog.Properties, _serializerOptions);

                                PrepareStringAttribute(groupId, key, value);
                            }

                            // file -> catalog -> resources
                            foreach (var catalogItem in catalogItemGroup)
                            {
                                (var chunkLength, var chunkCount) = GeneralHelper.CalculateChunkParameters(totalLength);
                                PrepareResource(groupId, catalogItem, chunkLength, chunkCount);
                            }
                        }
                        finally
                        {
                            if (H5I.is_valid(groupId) > 0) { _ = H5G.close(groupId); }
                        }
                    }
                }
                finally
                {
                    _ = H5F.flush(_fileId, H5F.scope_t.GLOBAL);
                }
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
                try
                {
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
                            WriteData(physicalId, offset, writeRequests[i]);
                        }

                        processed++;
                        progress.Report((double)processed / requests.Length);
                    }
                }
                finally
                {
                    _ = H5F.flush(_fileId, H5F.scope_t.GLOBAL);
                }
            }, cancellationToken);
        }

        public Task CloseAsync(CancellationToken cancellationToken)
        {
            if (H5I.is_valid(_fileId) > 0) { _ = H5F.close(_fileId); }

            return Task.CompletedTask;
        }

        private unsafe void WriteData(string catalogPhysicalId, ulong fileOffset, WriteRequest writeRequest)
        {
            long groupId = -1;
            long datasetId = -1;
            long dataspaceId = -1;
            long dataspaceId_Buffer = -1;

            try
            {
                var length = (ulong)writeRequest.Data.Length;
                groupId = H5G.open(_fileId, $"/{catalogPhysicalId}/{writeRequest.CatalogItem.Resource.Id}");

                var datasetName = $"dataset_{writeRequest.CatalogItem.Representation.Id}";
                datasetId = H5D.open(groupId, datasetName);
                dataspaceId = H5D.get_space(datasetId);
                dataspaceId_Buffer = H5S.create_simple(1, new ulong[] { length }, null);

                // dataset
                _ = H5S.select_hyperslab(dataspaceId,
                                    H5S.seloper_t.SET,
                                    new ulong[] { fileOffset },
                                    new ulong[] { 1 },
                                    new ulong[] { 1 },
                                    new ulong[] { length });

                fixed (byte* bufferPtr = MemoryMarshal.AsBytes(writeRequest.Data.Span))
                {
                    if (H5D.write(datasetId, H5T.NATIVE_DOUBLE, dataspaceId_Buffer, dataspaceId, H5P.DEFAULT, new IntPtr(bufferPtr)) < 0)
                        throw new Exception(ErrorMessage.Hdf5Writer_CouldNotWriteChunk_Dataset);
                }
            }
            finally
            {
                if (H5I.is_valid(groupId) > 0) { _ = H5G.close(groupId); }
                if (H5I.is_valid(datasetId) > 0) { _ = H5D.close(datasetId); }
                if (H5I.is_valid(dataspaceId) > 0) { _ = H5S.close(dataspaceId); }
                if (H5I.is_valid(dataspaceId_Buffer) > 0) { _ = H5S.close(dataspaceId_Buffer); }
            }
        }

        private static void PrepareResource(long locationId, CatalogItem catalogItem, ulong chunkLength, ulong chunkCount)
        {
            long groupId = -1;
            long datasetId = -1;

            try
            {
                if (chunkLength <= 0)
                    throw new Exception(ErrorMessage.Hdf5Writer_SampleRateTooLow);

                groupId = IOHelper.OpenOrCreateGroup(locationId, catalogItem.Resource.Id).GroupId;

                // file -> catalog -> resource -> properties
                if (catalogItem.Resource.Properties is not null)
                {
                    var key = "properties";
                    var value = JsonSerializer.Serialize(catalogItem.Resource.Properties, _serializerOptions);

                    PrepareStringAttribute(groupId, key, value);
                }

                // file -> catalog -> resource -> representation
                datasetId = OpenOrCreateRepresentation(groupId, $"dataset_{catalogItem.Representation.Id}{GetRepresentationParameterString(catalogItem.Parameters)}", chunkLength, chunkCount).DatasetId;
            }
            finally
            {
                if (H5I.is_valid(groupId) > 0) { _ = H5G.close(groupId); }
                if (H5I.is_valid(datasetId) > 0) { _ = H5D.close(datasetId); }
            }
        }

        // low level
        private static (long DatasetId, bool IsNew) OpenOrCreateRepresentation(long locationId, string name, ulong chunkLength, ulong chunkCount)
        {
            long datasetId = -1;
            GCHandle gcHandle_fillValue = default;
            bool isNew;

            try
            {
                var fillValue = Double.NaN;
                gcHandle_fillValue = GCHandle.Alloc(fillValue, GCHandleType.Pinned);

                (datasetId, isNew) = IOHelper.OpenOrCreateDataset(locationId, name, H5T.NATIVE_DOUBLE, chunkLength, chunkCount, gcHandle_fillValue.AddrOfPinnedObject());
            }
            catch (Exception)
            {
                if (H5I.is_valid(datasetId) > 0) { _ = H5D.close(datasetId); }

                throw;
            }
            finally
            {
                if (gcHandle_fillValue.IsAllocated)
                    gcHandle_fillValue.Free();
            }

            return (datasetId, isNew);
        }

        private static void PrepareStringAttribute(long locationId, string name, string value)
        {
            long typeId = -1;
            long attributeId = -1;

            bool isNew;

            try
            {
                var classNamePtr = Marshal.StringToHGlobalAnsi(value);

                typeId = H5T.copy(H5T.C_S1);
                _ = H5T.set_size(typeId, new IntPtr(value.Length));

                (attributeId, isNew) = IOHelper.OpenOrCreateAttribute(locationId, name, typeId, () =>
                {
                    long dataspaceId = -1;
                    long localAttributeId = -1;

                    try
                    {
                        dataspaceId = H5S.create(H5S.class_t.SCALAR);
                        localAttributeId = H5A.create(locationId, name, typeId, dataspaceId);
                    }
                    finally
                    {
                        if (H5I.is_valid(dataspaceId) > 0) { _ = H5S.close(dataspaceId); }
                    }

                    return localAttributeId;
                });

                if (isNew)
                    _ = H5A.write(attributeId, typeId, classNamePtr);
            }
            finally
            {
                if (H5I.is_valid(typeId) > 0) { _ = H5T.close(typeId); }
                if (H5I.is_valid(attributeId) > 0) { _ = H5A.close(attributeId); }
            }
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