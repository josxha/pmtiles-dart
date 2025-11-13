import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;
import 'dart:typed_data';

import 'archive.dart';
import 'exceptions.dart';
import 'header.dart';
import 'zxy.dart';

/// Result / statistics of a subset extraction.
class ExtractResult {
  final int requestedTiles;
  final int writtenTiles;
  final int skippedMissingTiles;
  final int bytesTileData;

  ExtractResult({
    required this.requestedTiles,
    required this.writtenTiles,
    required this.skippedMissingTiles,
    required this.bytesTileData,
  });

  @override
  String toString() => 'ExtractResult(requested=$requestedTiles, written=$writtenTiles, missing=$skippedMissingTiles, tileBytes=$bytesTileData)';
}

// Simple varint (LEB128 style) writer matching protobuf CodedBufferReader expectations.
void _writeVarint(List<int> out, int value) {
  // Only supports non-negative ints (as per our usage).
  while (true) {
    final byte = value & 0x7f;
    value >>= 7;
    if (value != 0) {
      out.add(byte | 0x80);
    } else {
      out.add(byte);
      break;
    }
  }
}

/// Extract a subset of tiles from [sourceArchivePath] and write a new PMTiles v3 archive
/// to [destinationPath], containing only the provided [tileIds].
///
/// Limitations of this minimal writer:
///  * Always clustered
///  * No leaf directories are generated (all entries in root)
///  * internalCompression is always `none`
///  * No tile de-duplication (every tile -> its own entry, runLength=1)
///  * Bounding box & center values are copied from the source archive (not recomputed)
///  * numberOfAddressedTiles/numberOfTileEntries/numberOfTileContents are all equal to the
///    number of written tile entries.
///  * Metadata can be overridden, otherwise '{}'.
///
/// Throws [ArgumentError] for invalid input or [CorruptArchiveException] if header+root exceed 16KB.
Future<ExtractResult> _extractSubset(
  String sourceArchivePath,
  String destinationPath,
  List<int> tileIds, {
  Map<String, dynamic>? metadataOverride,
}) async {
  if (tileIds.isEmpty) {
    throw ArgumentError('tileIds must not be empty');
  }
  // Deduplicate & sort
  final uniqueIds = tileIds.toSet().toList()..sort();

  final source = await PmTilesArchive.from(sourceArchivePath);
  try {
    final tiles = <int, List<int>>{}; // tileId -> compressed bytes
    int missing = 0;
    int totalTileBytes = 0;
    for (final id in uniqueIds) {
      final t = await source.tile(id);
      try {
        final bytes = t.compressedBytes();
        tiles[id] = bytes;
        totalTileBytes += bytes.length;
      } on TileNotFoundException {
        missing++;
      }
    }

    if (tiles.isEmpty) {
      throw ArgumentError('None of the requested tiles (${uniqueIds.length}) exist in source archive.');
    }

    final writtenIds = tiles.keys.toList()..sort();
    final n = writtenIds.length;

    // Compute header-derived stats from selected tiles
    int outMinZoom = 1 << 30;
    int outMaxZoom = -1;
    double minLon = 180.0;
    double maxLon = -180.0;
    double minLat = 90.0;
    double maxLat = -90.0;

    double tileXToLon(int x, int z) {
      final nTiles = 1 << z;
      return (x / nTiles) * 360.0 - 180.0;
    }

    double tileYToLat(int y, int z) {
      final nTiles = 1 << z;
      final a = math.pi * (1 - 2 * (y / nTiles));
      final sinhA = (math.exp(a) - math.exp(-a)) / 2.0;
      final latRad = math.atan(sinhA);
      return latRad * 180.0 / math.pi;
    }

    for (final id in writtenIds) {
      final zxy = ZXY.fromTileId(id);
      outMinZoom = math.min(outMinZoom, zxy.z);
      outMaxZoom = math.max(outMaxZoom, zxy.z);

      final west = tileXToLon(zxy.x, zxy.z);
      final east = tileXToLon(zxy.x + 1, zxy.z);
      final north = tileYToLat(zxy.y, zxy.z);
      final south = tileYToLat(zxy.y + 1, zxy.z);

      minLon = math.min(minLon, west);
      maxLon = math.max(maxLon, east);
      minLat = math.min(minLat, south);
      maxLat = math.max(maxLat, north);
    }

    // Build root directory buffer mirroring Directory.from expectations.
    final rootBytesInitial = <int>[];

    // Number of entries
    _writeVarint(rootBytesInitial, n);

    int lastId = 0;
    for (final id in writtenIds) {
      final delta = id - lastId;
      lastId = id;
      _writeVarint(rootBytesInitial, delta);
    }

    // runLength = 1 each
    for (var i = 0; i < n; i++) {
      _writeVarint(rootBytesInitial, 1);
    }

    // lengths
    for (final id in writtenIds) {
      _writeVarint(rootBytesInitial, tiles[id]!.length);
    }

    // offsets (offset+1 form to avoid zero special-case)
    int currentOffset = 0;
    for (final id in writtenIds) {
      _writeVarint(rootBytesInitial, currentOffset + 1);
      currentOffset += tiles[id]!.length;
    }

    bool useLeaf = false;
    List<int> rootBytes = rootBytesInitial;
    List<int> leafBytes = const <int>[]; // empty unless fallback

    final prospectiveRootLen = rootBytes.length;
    if (headerLength + prospectiveRootLen > headerAndRootMaxLength) {
      // Fallback: single leaf directory containing all tile entries.
      useLeaf = true;

      // Build leaf directory with all tile entries (same encoding as rootBytesInitial tile-only form)
      final leaf = <int>[];
      _writeVarint(leaf, n); // number of entries
      int lastId2 = 0;
      for (final id in writtenIds) {
        final delta = id - lastId2;
        lastId2 = id;
        _writeVarint(leaf, delta);
      }
      // runs = 1
      for (var i = 0; i < n; i++) {
        _writeVarint(leaf, 1);
      }
      // lengths
      for (final id in writtenIds) {
        _writeVarint(leaf, tiles[id]!.length);
      }
      // tile offsets
      int offset2 = 0;
      for (final id in writtenIds) {
        _writeVarint(leaf, offset2 + 1);
        offset2 += tiles[id]!.length;
      }
      leafBytes = leaf;

      // Build compact root with single leaf entry.
      rootBytes = <int>[];
      _writeVarint(rootBytes, 1); // one entry
      // delta for first tileId
      _writeVarint(rootBytes, writtenIds.first);
      // runs: single runLength=0 (leaf pointer)
      _writeVarint(rootBytes, 0);
      // length: leaf directory length
      _writeVarint(rootBytes, leafBytes.length);
      // offset: leaf directory offset +1 (0+1)
      _writeVarint(rootBytes, 1);

      if (headerLength + rootBytes.length > headerAndRootMaxLength) {
        throw CorruptArchiveException('Fallback root (len=${rootBytes.length}) still exceeds 16KB limit');
      }
    }

    final rootDirectoryOffset = headerLength; // 127 bytes
    final rootDirectoryLength = rootBytes.length;

    // Metadata JSON
    final metadata = metadataOverride ?? <String, dynamic>{};
    final metadataJson = json.encode(metadata);
    final metadataBytes = utf8.encode(metadataJson);

    final metadataOffset = rootDirectoryOffset + rootDirectoryLength;
    final metadataLength = metadataBytes.length;

    // Leaf directories (if used)
    final leafDirectoriesOffset = useLeaf ? metadataOffset + metadataLength : 0;
    final leafDirectoriesLength = useLeaf ? leafBytes.length : 0;

    final tileDataOffset = useLeaf
        ? leafDirectoriesOffset + leafDirectoriesLength
        : metadataOffset + metadataLength;
    final tileDataLength = totalTileBytes;

    // Build header
    final headerBytes = Uint8List(headerLength);
    final header = ByteData.view(headerBytes.buffer);

    // Magic 'PMTiles' + version 3
    final magic = utf8.encode('PMTiles');
    headerBytes.setRange(0, magic.length, magic);
    header.setUint8(0x07, 3); // version

    void setUint64(int offset, int value) {
      header.setUint32(offset, value & 0xffffffff, Endian.little);
      header.setUint32(offset + 4, (value >> 32) & 0xffffffff, Endian.little);
    }

    setUint64(0x08, rootDirectoryOffset);
    setUint64(0x10, rootDirectoryLength);
    setUint64(0x18, metadataOffset);
    setUint64(0x20, metadataLength);
    setUint64(0x28, leafDirectoriesOffset);
    setUint64(0x30, leafDirectoriesLength);
    setUint64(0x38, tileDataOffset);
    setUint64(0x40, tileDataLength);

    // Counts (simplified)
    setUint64(0x48, n); // numberOfAddressedTiles
    setUint64(0x50, useLeaf ? (n) : n); // numberOfTileEntries (leaf entries only if leaf used)
    setUint64(0x58, n); // numberOfTileContents

    // Flags & enums
    header.setUint8(0x60, 1); // clustered
    header.setUint8(0x61, 1); // internalCompression = none
    header.setUint8(0x62, source.tileCompression.index);
    header.setUint8(0x63, source.tileType.index);

    // Copy zooms & bounds
    header.setUint8(0x64, outMinZoom);
    header.setUint8(0x65, outMaxZoom);

    void writeLatLng(int offset, double lat, double lon) {
      final longitude = (lon * 10000000).round();
      final latitude = (lat * 10000000).round();
      header.setInt32(offset, longitude, Endian.little);
      header.setInt32(offset + 4, latitude, Endian.little);
    }

    writeLatLng(0x66, minLat, minLon);
    writeLatLng(0x6E, maxLat, maxLon);

    final centerLat = (minLat + maxLat) / 2.0;
    final centerLon = (minLon + maxLon) / 2.0;
    final centerZoom = outMinZoom; // simple heuristic
    header.setUint8(0x76, centerZoom);
    writeLatLng(0x77, centerLat, centerLon);

    // Write out file
    final file = File(destinationPath);
    final sink = file.openWrite();
    try {
      sink.add(headerBytes);
      sink.add(rootBytes);
      sink.add(metadataBytes);
      if (useLeaf) {
        sink.add(leafBytes);
      }
      for (final id in writtenIds) {
        sink.add(tiles[id]!);
      }
      await sink.flush();
    } finally {
      await sink.close();
    }

    return ExtractResult(
      requestedTiles: tileIds.length,
      writtenTiles: n,
      skippedMissingTiles: missing,
      bytesTileData: totalTileBytes,
    );
  } finally {
    await source.close();
  }
}

/// Extract tiles defined by a geographic bounding box and zoom range.
Future<ExtractResult> extractSubsetByBounds(
  String sourceArchivePath,
  String destinationPath, {
  double? west,
  double? south,
  double? east,
  double? north,
  int? minZoom,
  int? maxZoom,
  Map<String, dynamic>? metadataOverride,
}) async {
  // Open source to derive defaults when params are null
  final src = await PmTilesArchive.from(sourceArchivePath);
  try {
    final effMinZoom = (minZoom ?? src.minZoom).clamp(0, ZXY.maxAllowedZoom);
    final effMaxZoom = (maxZoom ?? src.maxZoom).clamp(0, ZXY.maxAllowedZoom);
    if (effMaxZoom < effMinZoom) {
      throw ArgumentError('Invalid zoom range: minZoom=$effMinZoom maxZoom=$effMaxZoom');
    }

    // Fill bbox from source header if not provided
    double effWest = west ?? src.minPosition.longitude;
    double effSouth = south ?? src.minPosition.latitude;
    double effEast = east ?? src.maxPosition.longitude;
    double effNorth = north ?? src.maxPosition.latitude;

    if (effSouth > effNorth) {
      throw ArgumentError('south must be <= north');
    }

    // Clamp latitude to Web Mercator limits
    const maxLat = 85.05112877980659;
    effSouth = effSouth.clamp(-maxLat, maxLat);
    effNorth = effNorth.clamp(-maxLat, maxLat);

    // Helper conversions
    double _lonToX(double lon, int z) {
      final n = 1 << z;
      final x = ((lon + 180.0) / 360.0) * n;
      return x;
    }

    double _latToY(double lat, int z) {
      final n = 1 << z;
      final latRad = lat * math.pi / 180.0;
      final y = (1 - math.log(math.tan(latRad) + 1 / math.cos(latRad)) / math.pi) / 2 * n;
      return y;
    }

    final tileIds = <int>{};
    for (int z = effMinZoom; z <= effMaxZoom; z++) {
      final n = 1 << z;

      // Compute ranges. y grows southward; north has smaller y than south.
      int yMin = _latToY(effNorth, z).floor();
      int yMax = _latToY(effSouth, z).floor();
      yMin = yMin.clamp(0, n - 1);
      yMax = yMax.clamp(0, n - 1);
      if (yMax < yMin) {
        final t = yMin;
        yMin = yMax;
        yMax = t;
      }

      // X can wrap the antimeridian; split if needed
      int westX = _lonToX(effWest, z).floor();
      int eastX = _lonToX(effEast, z).floor();
      westX = westX.clamp(0, n - 1);
      eastX = eastX.clamp(0, n - 1);

      List<(int,int)> xRanges;
      if (effWest <= effEast) {
        xRanges = [(westX, eastX)];
      } else {
        // bbox crosses 180 meridian
        xRanges = [(0, eastX), (westX, n - 1)];
      }

      for (final (xStart, xEnd) in xRanges) {
        for (int x = xStart; x <= xEnd; x++) {
          for (int y = yMin; y <= yMax; y++) {
            tileIds.add(ZXY(z, x, y).toTileId());
          }
        }
      }
    }

    return _extractSubset(sourceArchivePath, destinationPath, tileIds.toList(), metadataOverride: metadataOverride);
  } finally {
    await src.close();
  }
}
