import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;
import 'dart:typed_data';
import 'package:crypto/crypto.dart' as crypto;

import 'archive.dart';
import 'exceptions.dart';
import 'header.dart';
import 'zxy.dart';

// Location of a tile's bytes within the temp spool file
class _Loc {
  final int offset;
  final int length;
  const _Loc(this.offset, this.length);
}

class _Bounds {
  final double west;
  final double south;
  final double east;
  final double north;
  const _Bounds(this.west, this.south, this.east, this.north);
}

class _RunEntry {
  final int tileId;
  final int run;
  final int length;
  final int offset;
  const _RunEntry(this.tileId, this.run, this.length, this.offset);
}

/// Result / statistics of a subset extraction.
class ExtractResult {
  final int requestedTiles;
  final int writtenTiles;
  final int skippedMissingTiles;
  final int skippedEmptyTiles;
  final int bytesTileData;

  ExtractResult({
    required this.requestedTiles,
    required this.writtenTiles,
    required this.skippedMissingTiles,
    required this.skippedEmptyTiles,
    required this.bytesTileData,
  });

  @override
  String toString() => 'ExtractResult(requested=$requestedTiles, written=$writtenTiles, missing=$skippedMissingTiles, empty=$skippedEmptyTiles, tileBytes=$bytesTileData)';
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
  _Bounds? boundsOverride,
}) async {
  if (tileIds.isEmpty) {
    throw ArgumentError('tileIds must not be empty');
  }
  // Deduplicate & sort
  final uniqueIds = tileIds.toSet().toList()..sort();

  final source = await PmTilesArchive.from(sourceArchivePath);
  try {
    // Spool to temporary file
    final tempPath = '${destinationPath}.tiles.tmp';
    final tempFile = File(tempPath);
    final tempSink = tempFile.openWrite();
    final tileLoc = <int, _Loc>{};
    final contentIndex = <String, _Loc>{}; // hash -> location in temp
    final uniqueOrder = <String>[]; // hash order of first occurrence
    final idHash = <int, String>{}; // tileId -> hash
    int tempOffset = 0;
    int missing = 0;
    int skippedEmpty = 0;
    // Use batching stream API for efficient reads
    await for (final t in source.tiles(uniqueIds)) {
      try {
        final bytes = t.compressedBytes();
        if (bytes.isEmpty) {
          // Skip empty tiles entirely (don't index them)
          skippedEmpty++;
          continue;
        }
        // Deduplicate by SHA-256 of compressed bytes
        final hash = crypto.sha256.convert(bytes).toString();
        final existing = contentIndex[hash];
        if (existing != null) {
          tileLoc[t.id] = existing;
          idHash[t.id] = hash;
          continue;
        }
        tempSink.add(bytes);
        final loc = _Loc(tempOffset, bytes.length);
        tileLoc[t.id] = loc;
        contentIndex[hash] = loc;
        uniqueOrder.add(hash);
        idHash[t.id] = hash;
        tempOffset += bytes.length;
      } on TileNotFoundException {
        missing++;
      } catch (_) {
        missing++;
      }
    }
    await tempSink.flush();
    await tempSink.close();

    if (tileLoc.isEmpty) {
      throw ArgumentError('None of the requested tiles (${uniqueIds.length}) exist in source archive.');
    }

    final totalTileBytes = tempOffset;
    final writtenIds = tileLoc.keys.toList()..sort();
    final n = writtenIds.length;
    final uniqueCount = uniqueOrder.length;

    // Build mapping from content hash to final offset in output tile data
    final contentFinalOffset = <String, int>{};
    int acc = 0;
    for (final h in uniqueOrder) {
      contentFinalOffset[h] = acc;
      acc += contentIndex[h]!.length;
    }

    // Build run-length entries by coalescing adjacent tileIds with identical content
    final runEntries = <_RunEntry>[];
    int? curStart;
    int? curPrev;
    String? curHash;
    int? curLen;
    int? curOff;
    int curRun = 0;
    void flushRun() {
      if (curStart != null) {
        runEntries.add(_RunEntry(curStart!, curRun, curLen!, curOff!));
      }
      curStart = null;
      curPrev = null;
      curHash = null;
      curLen = null;
      curOff = null;
      curRun = 0;
    }
    for (final id in writtenIds) {
      final h = idHash[id]!;
      final off = contentFinalOffset[h]!;
      final len = contentIndex[h]!.length;
      if (curStart == null) {
        curStart = id;
        curPrev = id;
        curHash = h;
        curLen = len;
        curOff = off;
        curRun = 1;
        continue;
      }
      if (id == curPrev! + 1 && h == curHash && len == curLen && off == curOff) {
        curPrev = id;
        curRun++;
      } else {
        flushRun();
        curStart = id;
        curPrev = id;
        curHash = h;
        curLen = len;
        curOff = off;
        curRun = 1;
      }
    }
    flushRun();
    final entryCount = runEntries.length;

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

    // If a bbox was provided, prefer it for header bounds/center when it doesn't cross the antimeridian
    double headerMinLon = minLon;
    double headerMaxLon = maxLon;
    double headerMinLat = minLat;
    double headerMaxLat = maxLat;
    if (boundsOverride != null) {
      final b = boundsOverride;
      // only override if west <= east (no antimeridian crossing)
      if (b.west <= b.east) {
        headerMinLon = b.west;
        headerMaxLon = b.east;
        headerMinLat = b.south;
        headerMaxLat = b.north;
      }
    }

    // Build root directory buffer mirroring Directory.from expectations.
    final rootRaw = <int>[];
    _writeVarint(rootRaw, n);
    int lastId = 0;
    for (final id in writtenIds) {
      final delta = id - lastId;
      lastId = id;
      _writeVarint(rootRaw, delta);
    }
    for (var i = 0; i < n; i++) {
      _writeVarint(rootRaw, 1);
    }
    for (final id in writtenIds) {
      final h = idHash[id]!;
      _writeVarint(rootRaw, contentIndex[h]!.length);
    }
    int currentOffset = 0;
    for (final id in writtenIds) {
      final h = idHash[id]!;
      final off = contentFinalOffset[h]!;
      _writeVarint(rootRaw, off + 1);
    }

    // Build root directory candidates
    List<int> buildRootBytesFromIds() {
      final raw = <int>[];
      _writeVarint(raw, n);
      int lastId = 0;
      for (final id in writtenIds) {
        final delta = id - lastId;
        lastId = id;
        _writeVarint(raw, delta);
      }
      for (var i = 0; i < n; i++) {
        _writeVarint(raw, 1);
      }
      for (final id in writtenIds) {
        final h = idHash[id]!;
        _writeVarint(raw, contentIndex[h]!.length);
      }
      for (final id in writtenIds) {
        final h = idHash[id]!;
        final off = contentFinalOffset[h]!;
        _writeVarint(raw, off + 1);
      }
      return raw;
    }

    List<int> buildRootBytesFromRuns(List<_RunEntry> runs) {
      final raw = <int>[];
      _writeVarint(raw, runs.length);
      int lastId = 0;
      for (final e in runs) {
        final delta = e.tileId - lastId;
        lastId = e.tileId;
        _writeVarint(raw, delta);
      }
      for (final e in runs) { _writeVarint(raw, e.run); }
      for (final e in runs) { _writeVarint(raw, e.length); }
      for (final e in runs) { _writeVarint(raw, e.offset + 1); }
      return raw;
    }

    // Internal gzip compression with higher level
    final gzip = GZipCodec(level: 9);

    // Try root-only with all ids
    List<int> rootBytes = gzip.encode(buildRootBytesFromIds());
    bool usedRunsInRoot = false;

    // If too big, try root-only with run-length coalesced entries
    if (headerLength + rootBytes.length > headerAndRootMaxLength) {
      final rootBytesRun = gzip.encode(buildRootBytesFromRuns(runEntries));
      if (headerLength + rootBytesRun.length <= headerAndRootMaxLength) {
        rootBytes = rootBytesRun;
        usedRunsInRoot = true;
      }
    }

    bool useLeaf = false;
    List<int> leafBytes = const <int>[];

    // If still too big, fallback to leaves built from runs (more compact)
    if (headerLength + rootBytes.length > headerAndRootMaxLength) {
      useLeaf = true;
      // Segment leaf into chunks (e.g., 4096 entries per leaf)
      const maxLeafEntries = 4096;
      final leaves = <List<int>>[];
      int start = 0;
      while (start < entryCount) {
        final end = math.min(start + maxLeafEntries, entryCount);
        final part = runEntries.sublist(start, end);
        final raw = <int>[];
        _writeVarint(raw, part.length);
        int last = 0;
        for (final e in part) {
          final d = e.tileId - last;
          last = e.tileId;
          _writeVarint(raw, d);
        }
        for (final e in part) { _writeVarint(raw, e.run); }
        for (final e in part) { _writeVarint(raw, e.length); }
        for (final e in part) { _writeVarint(raw, e.offset + 1); }
        leaves.add(gzip.encode(raw));
        start = end;
      }

      // Build compact root with N leaf entries
      final rootComp = <int>[];
      _writeVarint(rootComp, leaves.length);
      for (int i = 0; i < leaves.length; i++) {
        final leafFirstId = runEntries[i * maxLeafEntries].tileId;
        final delta = i == 0 ? leafFirstId : (leafFirstId - runEntries[(i - 1) * maxLeafEntries].tileId);
        _writeVarint(rootComp, delta);
      }
      for (int i = 0; i < leaves.length; i++) { _writeVarint(rootComp, 0); }
      for (final lb in leaves) { _writeVarint(rootComp, lb.length); }
      int leafOff = 0;
      for (final lb in leaves) { _writeVarint(rootComp, leafOff + 1); leafOff += lb.length; }
      rootBytes = gzip.encode(rootComp);

      if (headerLength + rootBytes.length > headerAndRootMaxLength) {
        throw CorruptArchiveException('Compressed root still exceeds 16KB limit');
      }
      leafBytes = leaves.expand((e) => e).toList();
    }

    final rootDirectoryOffset = headerLength; // 127 bytes
    final rootDirectoryLength = rootBytes.length;

    // Metadata JSON: copy from source, merge overrides if provided
    Map<String, dynamic> baseMetadata = {};
    final srcMeta = await source.metadata;
    if (srcMeta is Map<String, dynamic>) {
      baseMetadata = Map<String, dynamic>.from(srcMeta);
    }
    final effectiveMetadata = metadataOverride == null
        ? baseMetadata
        : {...baseMetadata, ...metadataOverride};
    final metadataJson = json.encode(effectiveMetadata);
    final metadataBytes = gzip.encode(utf8.encode(metadataJson));

    final metadataOffset = rootDirectoryOffset + rootDirectoryLength;
    final metadataLength = metadataBytes.length;

    final leafDirectoriesOffset = useLeaf ? (metadataOffset + metadataLength) : 0;
    final leafDirectoriesLength = useLeaf ? leafBytes.length : 0;

    final int tileDataOffset = useLeaf
        ? (leafDirectoriesOffset + leafDirectoriesLength)
        : (metadataOffset + metadataLength);
    final tileDataLength = acc;

    // Build header
    final headerBytes = Uint8List(headerLength);
    final header = ByteData.view(headerBytes.buffer);
    final magic = utf8.encode('PMTiles');
    headerBytes.setRange(0, magic.length, magic);
    header.setUint8(0x07, 3);

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

    setUint64(0x48, n);
    setUint64(0x50, useLeaf ? entryCount : (usedRunsInRoot ? entryCount : n));
    setUint64(0x58, uniqueCount);

    // clustered & internalCompression=gzip
    header.setUint8(0x60, 1);
    header.setUint8(0x61, 2); // gzip
    header.setUint8(0x62, source.tileCompression.index);
    header.setUint8(0x63, source.tileType.index);

    header.setUint8(0x64, outMinZoom);
    header.setUint8(0x65, outMaxZoom);

    void writeLatLng(int offset, double lat, double lon) {
      final longitude = (lon * 10000000).round();
      final latitude = (lat * 10000000).round();
      header.setInt32(offset, longitude, Endian.little);
      header.setInt32(offset + 4, latitude, Endian.little);
    }

    writeLatLng(0x66, headerMinLat, headerMinLon);
    writeLatLng(0x6E, headerMaxLat, headerMaxLon);

    final centerLat = ((headerMinLat + headerMaxLat) / 2.0);
    final centerLon = ((headerMinLon + headerMaxLon) / 2.0);
    final centerZoom = outMinZoom;
    header.setUint8(0x76, centerZoom);
    writeLatLng(0x77, centerLat, centerLon);

    final file = File(destinationPath);
    final sink = file.openWrite();
    try {
      sink.add(headerBytes);
      sink.add(rootBytes);
      sink.add(metadataBytes);
      if (useLeaf) {
        sink.add(leafBytes);
      }
      final raf = await tempFile.open();
      try {
        // Write unique contents only, in order
        for (final h in uniqueOrder) {
          final loc = contentIndex[h]!;
          await raf.setPosition(loc.offset);
          final bytes = await raf.read(loc.length);
          sink.add(bytes);
        }
      } finally {
        await raf.close();
      }
      await sink.flush();
    } finally {
      await sink.close();
    }
    try { await tempFile.delete(); } catch (_) {}

    return ExtractResult(
      requestedTiles: tileIds.length,
      writtenTiles: n,
      skippedMissingTiles: missing,
      skippedEmptyTiles: skippedEmpty,
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

    final bounds = _Bounds(effWest, effSouth, effEast, effNorth);
    return _extractSubset(
      sourceArchivePath,
      destinationPath,
      tileIds.toList(),
      metadataOverride: metadataOverride,
      boundsOverride: bounds,
    );
  } finally {
    await src.close();
  }
}
