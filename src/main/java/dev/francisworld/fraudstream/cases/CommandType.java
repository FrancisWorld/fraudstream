package dev.francisworld.fraudstream.cases;

/** Actions an analyst can take on a case. */
public enum CommandType {
  CLAIM,
  RELEASE,
  CONFIRM_FRAUD,
  DISMISS,
  COMMENT
}
