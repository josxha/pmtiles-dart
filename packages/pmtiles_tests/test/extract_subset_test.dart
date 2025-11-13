import 'dart:io';
import 'package:test/test.dart';
import 'package:pmtiles/pmtiles.dart';

void main() {
  test('extract subset using bbox only', () async {
    final source = 'samples/countries.pmtiles';
    final dest = 'samples/_subset_bbox_only.pmtiles';
    final result = await extractSubsetByBounds(source, dest,
        west: -10, south: 35, east: 30, north: 60, minZoom: 2, maxZoom: 3);
    expect(result.writtenTiles, greaterThan(0));
    final subset = await PmTilesArchive.from(dest);
    await subset.close();
    await File(dest).delete();
  });
}
