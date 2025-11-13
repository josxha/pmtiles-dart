import 'dart:io';
import 'package:test/test.dart';
import 'package:pmtiles/pmtiles.dart';

void main() {
  test('extract subset defaults to source header when bbox/zoom omitted', () async {
    final source = 'samples/countries.pmtiles';
    final dest = 'samples/_subset_defaults.pmtiles';

    // Use tiny bbox but omit minZoom to test defaulting; restrict maxZoom to source minZoom to keep output small
    final src = await PmTilesArchive.from(source);
    final mz = src.minZoom;
    await src.close();

    final result = await extractSubsetByBounds(
      source,
      dest,
      west: -10.0,
      south: 35.0,
      east: -9.8,
      north: 35.2,
      // minZoom omitted -> default to src.minZoom
      maxZoom: mz,
    );

    expect(result.writtenTiles, greaterThan(0));

    final src2 = await PmTilesArchive.from(source);
    final subset = await PmTilesArchive.from(dest);
    try {
      // Header zooms and bounds should be within or equal to source (since we extracted full range by default)
      expect(subset.minZoom, greaterThanOrEqualTo(src2.minZoom));
      expect(subset.maxZoom, lessThanOrEqualTo(src2.maxZoom));
    } finally {
      await src2.close();
      await subset.close();
    }

    await File(dest).delete();
  });
}
