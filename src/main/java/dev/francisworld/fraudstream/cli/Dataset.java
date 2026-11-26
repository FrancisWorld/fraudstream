package dev.francisworld.fraudstream.cli;

import dev.francisworld.fraudstream.serde.Json;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** JSON Lines persistence for labelled datasets. */
public final class Dataset {

  private Dataset() {}

  public static void write(Path file, List<Labelled> items) throws IOException {
    try (BufferedWriter out = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      for (Labelled item : items) {
        out.write(Json.writeString(item));
        out.newLine();
      }
    }
  }

  public static List<Labelled> read(Path file) throws IOException {
    List<Labelled> items = new ArrayList<>();
    try (BufferedReader in = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line;
      int number = 0;
      while ((line = in.readLine()) != null) {
        number++;
        if (line.isBlank()) {
          continue;
        }
        try {
          items.add(Json.MAPPER.readValue(line, Labelled.class));
        } catch (IOException | IllegalArgumentException e) {
          throw new IOException(
              "Line " + number + " of " + file + " is not a valid record: " + e.getMessage(), e);
        }
      }
    }
    return items;
  }
}
