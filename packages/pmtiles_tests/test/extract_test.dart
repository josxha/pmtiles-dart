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

          // Fetch a few tiles derived from header bbox at minZoom and verify bytes are readable
          final ids = <int>[];
          final z = subset.header.minZoom;
          // Helper conversions
          int lonToX(double lon, int z) {
            final n = 1 << z;
            return (((lon + 180.0) / 360.0) * n).floor().clamp(0, n - 1);
          }
          int latToY(double lat, int z) {
            final n = 1 << z;
            final latRad = lat * math.pi / 180.0;
            final y = (1 - math.log(math.tan(latRad) + 1 / math.cos(latRad)) / math.pi) / 2 * n;
            return y.floor().clamp(0, n - 1);
          }
          final west = subset.header.minPosition.longitude;
          final south = subset.header.minPosition.latitude;
          final east = subset.header.maxPosition.longitude;
          final north = subset.header.maxPosition.latitude;
          final xStart = lonToX(west, z);
          final xEnd = lonToX(east, z);
          final yStart = latToY(north, z);
          final yEnd = latToY(south, z);
          for (int x = math.min(xStart, xEnd); x <= math.max(xStart, xEnd); x++) {
            for (int y = math.min(yStart, yEnd); y <= math.max(yStart, yEnd); y++) {
              ids.add(ZXY(z, x, y).toTileId());
              if (ids.length >= 8) break;
            }
            if (ids.length >= 8) break;
          }
          expect(ids, isNotEmpty, reason: 'should have at least one candidate tile id');

          // Single fetch
          final ok = <int>[];
          for (final id in ids) {
            try {
              final t = await subset.tile(id);
              final bytes = t.compressedBytes();
              if (bytes.isNotEmpty) ok.add(id);
            } catch (_) {
              // ignore missing/empty, try next
            }
            if (ok.length >= 3) break;
          }
          expect(ok, isNotEmpty, reason: 'should be able to read at least one real tile payload');

          // Batch fetch via tiles([...]) stream
          final wanted = ok.take(3).toList();
          final got = <int, List<int>>{};
          await for (final t in subset.tiles(wanted)) {
            got[t.id] = t.compressedBytes();
          }
          expect(got.keys.toSet(), equals(wanted.toSet()));
          for (final id in wanted) {
            expect(got[id]!, isNotEmpty, reason: 'batch tile $id should have non-empty payload');
          }
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

  group('metadata copy and overrides', () {
    test('copies metadata from source by default', () async {
      final source = 'samples/countries.pmtiles';
      final dest = 'samples/_extract_meta_copy.pmtiles';
      addTearDown(() async {
        final f = File(dest);
        if (await f.exists()) {
          await f.delete();
        }
      });

      final src = await PmTilesArchive.from(source);
      final srcMeta = await src.metadata;
      await src.close();

      final result = await extractSubsetByBounds(source, dest);
      expect(result.writtenTiles, greaterThan(0));

      final subset = await PmTilesArchive.from(dest);
      try {
        final dstMeta = await subset.metadata;
        expect(dstMeta, isA<Map<String, dynamic>>());
        expect(dstMeta, equals(srcMeta));
      } finally {
        await subset.close();
      }
    });

    test('merges metadata overrides over source metadata', () async {
      final source = 'samples/countries.pmtiles';
      final dest = 'samples/_extract_meta_override.pmtiles';
      addTearDown(() async {
        final f = File(dest);
        if (await f.exists()) {
          await f.delete();
        }
      });

      final override = {'test_key': 'test_value'};
      final result = await extractSubsetByBounds(source, dest, metadataOverride: override);
      expect(result.writtenTiles, greaterThan(0));

      final subset = await PmTilesArchive.from(dest);
      try {
        final dstMeta = await subset.metadata as Map<String, dynamic>;
        expect(dstMeta['test_key'], equals('test_value'));
      } finally {
        await subset.close();
      }
    });
  });

  group('full archive integrity', () {
    test('all addressed tiles can be enumerated and fetched', () async {
      final source = 'samples/countries.pmtiles';
      final dest = 'samples/_extract_full_integrity.pmtiles';

      final result = await extractSubsetByBounds(source, dest);
      expect(result.writtenTiles, greaterThan(0));

      final subset = await PmTilesArchive.from(dest);
      try {
        final ids = await subset.addressedTileIds();
        expect(ids.length, equals(subset.header.numberOfAddressedTiles));
        expect(ids, isNotEmpty);
        for (int i = 1; i < ids.length; i++) {
          expect(ids[i] > ids[i - 1], isTrue, reason: 'IDs must be strictly increasing');
        }

        // Zufällige Stichprobe: 20 eindeutige IDs mit festem Seed
        final rand = math.Random(42);
        final sampleSet = <int>{};
        while (sampleSet.length < math.min(20, ids.length)) {
          sampleSet.add(ids[rand.nextInt(ids.length)]);
        }
        final sample = sampleSet.toList();

        final fetchedRandom = <int, List<int>>{};
        await for (final t in subset.tiles(sample)) {
          try {
            final b = t.compressedBytes();
            if (b.isNotEmpty) fetchedRandom[t.id] = b;
          } catch (_) {}
        }
        expect(fetchedRandom.keys.toSet(), equals(sampleSet), reason: 'All random sampled tiles should be fetched');

        // Deterministische erste 50 IDs Batch
        final sample50 = ids.take(50).toList();
        final fetched = <int, List<int>>{};
        await for (final t in subset.tiles(sample50)) {
          try {
            final b = t.compressedBytes();
            if (b.isNotEmpty) fetched[t.id] = b;
          } catch (_) {}
        }
        expect(fetched.keys.length, greaterThan(0));
        expect(fetched.keys.toSet(), equals(sample50.toSet()));

        expect(subset.header.numberOfTileContents, lessThanOrEqualTo(subset.header.numberOfAddressedTiles));
        expect(subset.header.numberOfTileEntries, lessThanOrEqualTo(subset.header.numberOfAddressedTiles));
      } finally {
        await subset.close();
      }
      await File(dest).delete();
    });
  });
}
