package dev.francisworld.fraudstream.features;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;

class GeoMathTest {

  @Test
  void knownCityPairsAreCloseToPublishedDistances() {
    // Sao Paulo to Rio de Janeiro is roughly 360 km, London to New York roughly 5,570 km.
    assertThat(GeoMath.distanceKm(-23.55, -46.63, -22.90, -43.17)).isCloseTo(360, within(10.0));
    assertThat(GeoMath.distanceKm(51.51, -0.13, 40.71, -74.01)).isCloseTo(5_570, within(40.0));
  }

  @Test
  void samePointIsZeroAndAntipodesAreHalfTheCircumference() {
    assertThat(GeoMath.distanceKm(10, 20, 10, 20)).isZero();
    assertThat(GeoMath.distanceKm(0, 0, 0, 180)).isCloseTo(20_015, within(5.0));
  }

  @Provide
  Arbitrary<Double> lat() {
    return Arbitraries.doubles().between(-90, 90);
  }

  @Provide
  Arbitrary<Double> lon() {
    return Arbitraries.doubles().between(-180, 180);
  }

  @Property(tries = 300)
  void distanceIsSymmetricNonNegativeAndBounded(
      @ForAll("lat") double a,
      @ForAll("lon") double b,
      @ForAll("lat") double c,
      @ForAll("lon") double d) {
    double forward = GeoMath.distanceKm(a, b, c, d);
    assertThat(forward).isBetween(0.0, 20_040.0);
    assertThat(GeoMath.distanceKm(c, d, a, b)).isCloseTo(forward, within(1e-6));
  }
}
