import 'dart:io';
import 'package:test/test.dart';
import 'package:pmtiles/pmtiles.dart';

void main() {
  test('extract subset by bbox + zoom range', () async {
    final source = 'samples/countries.pmtiles';
    final dest = 'samples/_subset_bbox.pmtiles';

    // Small bbox around Europe at low zooms to keep tile count modest
    final result = await extractSubsetByBounds(
      source,
      dest,
      west: -10.0,
      south: 35.0,
      east: 30.0,
      north: 60.0,
      minZoom: 2,
      maxZoom: 3,
    );

    expect(result.writtenTiles, greaterThan(0));

    final subset = await PmTilesArchive.from(dest);
    try {
      expect(subset.header.numberOfTileEntries, result.writtenTiles);
      expect(subset.header.tileCompression, isNot(TileType.unknown));
      // Verify header values updated
      expect(subset.header.minZoom, greaterThanOrEqualTo(2));
      expect(subset.header.maxZoom, lessThanOrEqualTo(3));
      // Bounds should roughly encompass requested bbox (allow small numeric tolerance)
      final tol = 0.5; // degrees tolerance
      expect(subset.header.minPosition.longitude, lessThanOrEqualTo(-10.0 + tol));
      expect(subset.header.minPosition.latitude, lessThanOrEqualTo(35.0 + tol));
      expect(subset.header.maxPosition.longitude, greaterThanOrEqualTo(30.0 - tol));
      expect(subset.header.maxPosition.latitude, greaterThanOrEqualTo(60.0 - tol));
    } finally {
      await subset.close();
    }

    await File(dest).delete();
  });
}
