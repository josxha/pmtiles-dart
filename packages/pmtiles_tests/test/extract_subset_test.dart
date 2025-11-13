import 'dart:io';
import 'package:test/test.dart';
import 'package:pmtiles/pmtiles.dart';

void main() {
  test('extract subset single tile', () async {
    final source = 'samples/countries.pmtiles';
    final dest = 'samples/_subset_countries.pmtiles';
    // Choose first tile id from min zoom
    final srcArchive = await PmTilesArchive.from(source);
    final firstId = ZXY(srcArchive.minZoom, 0, 0).toTileId();
    await srcArchive.close();

    final result = await extractSubset(source, dest, [firstId]);
    expect(result.writtenTiles, 1);

    final subset = await PmTilesArchive.from(dest);
    try {
      final tile = await subset.tile(firstId);
      final orig = await PmTilesArchive.from(source);
      try {
        final origTile = await orig.tile(firstId);
        expect(tile.compressedBytes(), origTile.compressedBytes());
        expect(subset.header.tileCompression, orig.header.tileCompression);
        expect(subset.header.tileType, orig.header.tileType);
        expect(subset.header.numberOfTileEntries, 1);
      } finally {
        await orig.close();
      }
    } finally {
      await subset.close();
    }

    // Cleanup
    await File(dest).delete();
  });

  test('extract subset multiple tiles', () async {
    final source = 'samples/countries.pmtiles';
    final dest = 'samples/_subset_multi.pmtiles';
    final srcArchive = await PmTilesArchive.from(source);
    final baseId = ZXY(srcArchive.minZoom, 0, 0).toTileId();
    await srcArchive.close();

    final ids = [baseId, baseId + 1, baseId + 1, baseId + 2]; // include duplicate
    final result = await extractSubset(source, dest, ids);
    expect(result.writtenTiles, 3); // duplicate removed

    final subset = await PmTilesArchive.from(dest);
    try {
      for (final id in {baseId, baseId + 1, baseId + 2}) {
        final tile = await subset.tile(id);
        final origArchive = await PmTilesArchive.from(source);
        try {
          final origTile = await origArchive.tile(id);
          expect(tile.compressedBytes(), origTile.compressedBytes());
        } finally {
          await origArchive.close();
        }
      }
      expect(subset.header.numberOfTileEntries, 3);
    } finally {
      await subset.close();
    }

    await File(dest).delete();
  });

  test('extract subset with missing tiles', () async {
    final source = 'samples/countries.pmtiles';
    final dest = 'samples/_subset_missing.pmtiles';
    final srcArchive = await PmTilesArchive.from(source);
    final minId = ZXY(srcArchive.minZoom, 0, 0).toTileId();
    final maxPossibleId = ZXY(srcArchive.maxZoom + 1, 0, 0).toTileId(); // outside range
    await srcArchive.close();

    final ids = [minId, maxPossibleId];
    final result = await extractSubset(source, dest, ids);
    expect(result.writtenTiles, 1);
    expect(result.skippedMissingTiles, 1);

    final subset = await PmTilesArchive.from(dest);
    try {
      final tile = await subset.tile(minId);
      final origArchive = await PmTilesArchive.from(source);
      try {
        final origTile = await origArchive.tile(minId);
        expect(tile.compressedBytes(), origTile.compressedBytes());
      } finally {
        await origArchive.close();
      }
    } finally {
      await subset.close();
    }

    await File(dest).delete();
  });
}
