package dev.francisworld.fraudstream.api;

/** An authenticated caller. The name is what lands in case audit trails. */
public record Principal(String name, Role role) {

  public boolean can(Permission permission) {
    return role.allows(permission);
  }
}
