package dev.francisworld.fraudstream.api;

import java.util.EnumSet;
import java.util.Set;

/** Roles bundle permissions; a token carries exactly one role. */
public enum Role {
  /** Upstream systems that submit transactions and nothing else. */
  INGEST(EnumSet.of(Permission.INGEST_TRANSACTIONS)),
  /** Fraud analysts: read everything and work cases. */
  ANALYST(EnumSet.of(Permission.READ, Permission.WORK_CASES)),
  /** Operators: everything, including publishing rule sets. */
  ADMIN(EnumSet.allOf(Permission.class));

  private final Set<Permission> permissions;

  Role(Set<Permission> permissions) {
    this.permissions = permissions;
  }

  public boolean allows(Permission permission) {
    return permissions.contains(permission);
  }
}
