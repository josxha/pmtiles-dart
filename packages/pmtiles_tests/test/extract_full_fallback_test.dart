import 'dart:io';
import 'package:test/test.dart';
import 'package:pmtiles/pmtiles.dart';

void main() {
  test('full archive extraction uses leaf fallback when root too large', () async {
    final source = 'samples/countries.pmtiles';
    final dest = 'samples/_full_extract.pmtiles';

    final result = await extractSubsetByBounds(source, dest); // no params -> full
    expect(result.writtenTiles, greaterThan(1000));

    final extracted = await PmTilesArchive.from(dest);
    try {
      expect(extracted.header.rootDirectoryLength, lessThan(16384));
      expect(extracted.header.leafDirectoriesLength, greaterThan(0));
      expect(extracted.header.numberOfTileEntries, result.writtenTiles);
    } finally {
      await extracted.close();
    }
    await File(dest).delete();
  });
}

