package com.acme.customeringest.service;

public enum ProcessingOutcome {
  PROCESSED,
  DUPLICATE_SKIPPED,
  LOCK_NOT_ACQUIRED,
  FAILED
}
