// ignore_for_file: avoid_print

import 'dart:convert';
import 'dart:io';
import 'package:args/command_runner.dart';
import 'package:pmtiles/pmtiles.dart';

class ZxyCommand extends Command {
  @override
  final name = 'zxy';

  @override
  final description = 'Converts between tile ID and Z X Y.';

  @override
  String get invocation {
    return 'pmtiles zxy <tileId>\n'
        '   or: pmtiles zxy <z> <x> <y>';
  }

  @override
  void run() async {
    if (argResults!.rest.length == 1) {
      final tileId = int.parse(argResults!.rest[0]);

      print(ZXY.fromTileId(tileId));

      return;
    }

    if (argResults!.rest.length == 3) {
      final z = int.parse(argResults!.rest[0]);
      final x = int.parse(argResults!.rest[1]);
      final y = int.parse(argResults!.rest[2]);

      print(ZXY(z, x, y).toTileId());

      return;
    }

    throw UsageException('', usage);
  }
}

class ShowCommand extends Command {
  @override
  final name = 'show';
  @override
  final description = 'Show metadata related to a archive.';

  ShowCommand() {
    argParser.addFlag('show-metadata', defaultsTo: true, aliases: ['m']);
    argParser.addFlag('show-root', defaultsTo: false, aliases: ['r']);
  }

  @override
  String get invocation {
    return 'pmtiles show <archive>';
  }

  @override
  void run() async {
    if (argResults!.rest.length != 1) {
      throw UsageException('Must provide a single archive', usage);
    }

    final file = argResults!.rest[0];
    final tiles = await PmTilesArchive.from(file);
    try {
      print('Header:');
      print(tiles.header);

      if (argResults!['show-metadata']) {
        print('Metadata:');

        final encoder = JsonEncoder.withIndent('  ');
        String prettyJson = encoder.convert(await tiles.metadata);
        print(prettyJson);
      }

      if (argResults!['show-root']) {
        print('Root:');
        print('  ${tiles.root}');
      }
    } finally {
      await tiles.close();
    }
  }
}

class TileCommand extends Command {
  @override
  final name = 'tile';
  @override
  final description =
      'Fetch one tile from a local or remote archive and output on stdout.';

  TileCommand() {
    argParser.addFlag('uncompress', defaultsTo: true);
  }

  @override
  String get invocation {
    return 'pmtiles tile [<options>] <archive> <tileId>';
  }

  @override
  void run() async {
    if (argResults!.rest.length != 2) {
      throw UsageException('', usage);
    }

    final file = argResults!.rest[0];
    final tileId = int.parse(argResults!.rest[1]);

    final tiles = await PmTilesArchive.from(file);
    try {
      // Write the binary tile to stdout.
      final tile = await tiles.tile(tileId);
      IOSink(stdout).add(tile.bytes());
    } finally {
      await tiles.close();
    }
  }
}

class ExtractCommand extends Command {
  @override
  final name = 'extract';
  @override
  final description = 'Extract a subset of tiles into a new archive';

  ExtractCommand() {
    argParser
      ..addOption('metadata', help: 'Path to JSON metadata override')
      ..addOption('bbox', help: 'Bounding box as west,south,east,north (optional; defaults to source bounds)')
      ..addOption('minzoom', help: 'Minimum zoom for bbox mode (optional; defaults to source header)')
      ..addOption('maxzoom', help: 'Maximum zoom for bbox mode (optional; defaults to source header)');
  }

  @override
  String get invocation => 'pmtiles extract [--metadata <file.json>] [--bbox W,S,E,N] [--minzoom Z] [--maxzoom Z] <source> <dest>';

  @override
  void run() async {
    if (argResults!.rest.length < 2) {
      throw UsageException('Need <source> <dest>', usage);
    }
    final source = argResults!.rest[0];
    final dest = argResults!.rest[1];

    Map<String, dynamic>? metadata;
    final metadataPath = argResults!['metadata'] as String?;
    if (metadataPath != null) {
      final content = await File(metadataPath).readAsString();
      metadata = json.decode(content) as Map<String, dynamic>;
    }

    final bboxStr = argResults!['bbox'] as String?;
    final minZoomStr = argResults!['minzoom'] as String?;
    final maxZoomStr = argResults!['maxzoom'] as String?;

    if (bboxStr != null || minZoomStr != null || maxZoomStr != null) {
      double? west;
      double? south;
      double? east;
      double? north;
      if (bboxStr != null) {
        final parts = bboxStr.split(',');
        if (parts.length != 4) {
          throw UsageException('Invalid --bbox format, expected west,south,east,north', usage);
        }
        west = double.parse(parts[0]);
        south = double.parse(parts[1]);
        east = double.parse(parts[2]);
        north = double.parse(parts[3]);
      }
      final int? minZ = minZoomStr != null ? int.parse(minZoomStr) : null;
      final int? maxZ = maxZoomStr != null ? int.parse(maxZoomStr) : null;

      final result = await extractSubsetByBounds(
        source,
        dest,
        west: west,
        south: south,
        east: east,
        north: north,
        minZoom: minZ,
        maxZoom: maxZ,
        metadataOverride: metadata,
      );
      stderr.writeln('Wrote ${result.writtenTiles} tiles (requested ${result.requestedTiles}, missing ${result.skippedMissingTiles}, empty ${result.skippedEmptyTiles}) to $dest');
      return;
    }

    // Default: derive bbox+zoom from source (full archive extraction)
    final result = await extractSubsetByBounds(source, dest);
    stderr.writeln('Wrote ${result.writtenTiles} tiles to $dest (full archive; missing ${result.skippedMissingTiles}, empty ${result.skippedEmptyTiles})');
  }
}

main(List<String> args) async {
  CommandRunner('pmtiles', 'A pmtiles command line tool (written in dart).')
    ..addCommand(ShowCommand())
    ..addCommand(TileCommand())
    ..addCommand(ZxyCommand())
    ..addCommand(ExtractCommand())
    ..run(args).catchError((error) {
      if (error is! UsageException) throw error;
      print(error);
      exit(-1);
    });
}
