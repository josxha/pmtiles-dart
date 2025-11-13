import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'archive.dart';
import 'exceptions.dart';
import 'header.dart';
import 'types.dart';
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
Future<ExtractResult> extractSubset(
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

    // Build root directory buffer mirroring Directory.from expectations.
    final rootBytes = <int>[];

    // Number of entries
    _writeVarint(rootBytes, n);

    int lastId = 0;
    for (final id in writtenIds) {
      final delta = id - lastId;
      lastId = id;
      _writeVarint(rootBytes, delta);
    }

    // runLength = 1 each
    for (var i = 0; i < n; i++) {
      _writeVarint(rootBytes, 1);
    }

    // lengths
    for (final id in writtenIds) {
      _writeVarint(rootBytes, tiles[id]!.length);
    }

    // offsets (offset+1 form to avoid zero special-case)
    int currentOffset = 0;
    for (final id in writtenIds) {
      _writeVarint(rootBytes, currentOffset + 1);
      currentOffset += tiles[id]!.length;
    }

    final rootDirectoryOffset = headerLength; // 127 bytes
    final rootDirectoryLength = rootBytes.length;

    if (rootDirectoryOffset + rootDirectoryLength > headerAndRootMaxLength) {
      throw CorruptArchiveException('Root directory (len=$rootDirectoryLength) together with header exceeds 16KB limit');
    }

    // Metadata JSON
    final metadata = metadataOverride ?? <String, dynamic>{};
    final metadataJson = json.encode(metadata);
    final metadataBytes = utf8.encode(metadataJson);

    final metadataOffset = rootDirectoryOffset + rootDirectoryLength;
    final metadataLength = metadataBytes.length;

    // No leaf directories
    const leafDirectoriesOffset = 0;
    const leafDirectoriesLength = 0;

    final tileDataOffset = metadataOffset + metadataLength;
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
    setUint64(0x50, n); // numberOfTileEntries
    setUint64(0x58, n); // numberOfTileContents

    // Flags & enums
    header.setUint8(0x60, 1); // clustered
    header.setUint8(0x61, 1); // internalCompression = none
    header.setUint8(0x62, source.tileCompression.index);
    header.setUint8(0x63, source.tileType.index);

    // Copy zooms & bounds
    header.setUint8(0x64, source.minZoom);
    header.setUint8(0x65, source.maxZoom);

    void writeLatLng(int offset, double lat, double lon) {
      final longitude = (lon * 10000000).round();
      final latitude = (lat * 10000000).round();
      header.setInt32(offset, longitude, Endian.little);
      header.setInt32(offset + 4, latitude, Endian.little);
    }

    writeLatLng(0x66, source.minPosition.latitude, source.minPosition.longitude);
    writeLatLng(0x6E, source.maxPosition.latitude, source.maxPosition.longitude);

    header.setUint8(0x76, source.centerZoom);
    writeLatLng(0x77, source.centerPosition.latitude, source.centerPosition.longitude);

    // Write out file
    final file = File(destinationPath);
    final sink = file.openWrite();
    try {
      sink.add(headerBytes);
      sink.add(rootBytes);
      sink.add(metadataBytes);
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
