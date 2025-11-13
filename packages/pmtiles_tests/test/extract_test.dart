import 'dart:io';
import 'dart:math' as math;
import 'package:test/test.dart';
import 'package:pmtiles/pmtiles.dart';

void main() {
  group('extract by bbox and zooms', () {
    test('writes header bounds, center and counts', () async {
      final source = 'samples/countries.pmtiles';
      final dest = 'samples/_extract_bbox.pmtiles';

      final src = await PmTilesArchive.from(source);
      await src.close();

      final result = await extractSubsetByBounds(
        source,
        dest,
        west: 0,
        south: 0,
        east: 180,
        north: 90,
        minZoom: 0,
        maxZoom: 6,
      );

      expect(result.writtenTiles, greaterThan(0));

      final subset = await PmTilesArchive.from(dest);
      try {
        // Header bounds should reflect provided bbox (north clamped to WebMercator)
        expect(subset.header.minPosition.longitude, closeTo(0.0, 1e-6));
        expect(subset.header.minPosition.latitude, closeTo(0.0, 1e-6));
        expect(subset.header.maxPosition.longitude, closeTo(180.0, 1e-6));
        expect(subset.header.maxPosition.latitude, closeTo(85.051129, 1e-6));

        // Center = average
        expect(subset.header.centerPosition.longitude, closeTo(90.0, 1e-6));
        expect(subset.header.centerPosition.latitude, closeTo((0.0 + 85.051129) / 2.0, 1e-6));

        // Zooms within requested range
        expect(subset.header.minZoom, greaterThanOrEqualTo(0));
        expect(subset.header.maxZoom, lessThanOrEqualTo(6));

        // Internal compression and dedup counts
        expect(subset.header.internalCompression, Compression.gzip);
        expect(subset.header.numberOfTileContents, lessThanOrEqualTo(subset.header.numberOfTileEntries));
      } finally {
        await subset.close();
      }

      await File(dest).delete();
    });
  });

  group('extract defaults / full copy', () {
    test('no bbox/zoom uses source header and succeeds', () async {
      final source = 'samples/countries.pmtiles';
      final dest = 'samples/_extract_full.pmtiles';

      final src = await PmTilesArchive.from(source);
      try {
        final result = await extractSubsetByBounds(source, dest);
        expect(result.writtenTiles, greaterThan(0));

        final subset = await PmTilesArchive.from(dest);
        try {
          // Zooms equal or within source bounds
          expect(subset.header.minZoom, greaterThanOrEqualTo(src.header.minZoom));
          expect(subset.header.maxZoom, lessThanOrEqualTo(src.header.maxZoom));

          // Root compressed and leaf directories present for full copy (likely)
          expect(subset.header.rootDirectoryLength, lessThan(16384));
          expect(subset.header.numberOfTileContents, lessThanOrEqualTo(subset.header.numberOfTileEntries));
        } finally {
          await subset.close();
        }
      } finally {
        await src.close();
      }

      await File(dest).delete();
    });
  });

  group('extract with small bbox', () {
    test('bbox only, limited area', () async {
      final source = 'samples/countries.pmtiles';
      final dest = 'samples/_extract_small_bbox.pmtiles';

      final result = await extractSubsetByBounds(
        source,
        dest,
        west: -10,
        south: 35,
        east: -9.8,
        north: 35.2,
      );

      expect(result.writtenTiles, greaterThan(0));

      final subset = await PmTilesArchive.from(dest);
      try {
        expect(subset.header.minPosition.longitude, closeTo(-10.0, 1e-6));
        expect(subset.header.minPosition.latitude, closeTo(35.0, 1e-6));
        expect(subset.header.maxPosition.longitude, closeTo(-9.8, 1e-6));
        expect(subset.header.maxPosition.latitude, closeTo(35.2, 1e-6));
        expect(subset.header.internalCompression, Compression.gzip);
      } finally {
        await subset.close();
      }

      await File(dest).delete();
    });
  });
}

