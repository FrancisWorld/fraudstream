package dev.francisworld.fraudstream.features;

/** Great-circle helpers. */
public final class GeoMath {

  private static final double EARTH_RADIUS_KM = 6371.0088;

  private GeoMath() {}

  /** Haversine distance in kilometres. */
  public static double distanceKm(double lat1, double lon1, double lat2, double lon2) {
    double p1 = Math.toRadians(lat1);
    double p2 = Math.toRadians(lat2);
    double dp = p2 - p1;
    double dl = Math.toRadians(lon2 - lon1);
    double a =
        Math.sin(dp / 2) * Math.sin(dp / 2)
            + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) * Math.sin(dl / 2);
    return 2 * EARTH_RADIUS_KM * Math.asin(Math.min(1.0, Math.sqrt(a)));
  }
}
