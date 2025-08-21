# GBError Handling Strategy

This document defines a consistent strategy for error handling across the system using `*GBError` and standard Go `error` values.

## Goals

- Ensure errors that need to be inspected or acted upon via `errors.Is` are structured
- Maintain compatibility with Go idioms (`errors.Is`, `errors.As`, `%w`)
- Support error transmission over the network with a consistent format
- Separate internal error flows from structured error handling paths

---

## When to Use GBError vs fmt.Errorf

Use `*GBError` **only** when:

- You expect a consumer to inspect the error via `errors.Is`, `errors.As`, or error codes
- You want to encode the error for network transmission (`.Net()`, `.ToBytes()`)
- The error should be categorized by type (NETWORK/INTERNAL/SYSTEM)

Use `fmt.Errorf(...)` when:

- You need to return early on a basic failure
- You don't care about wrapping or inspection
- You're returning a one-off/local error that's not part of a larger system protocol

---

## Error Classifications

| Level                | Description                               | Use Case Example                     |
| -------------------- | ----------------------------------------- | ------------------------------------ |
| `NETWORK_ERR_LEVEL`  | Errors from other nodes or remote issues  | Peer failed to respond               |
| `INTERNAL_ERR_LEVEL` | Protocol errors, state violations         | Config mismatch, unexpected state    |
| `SYSTEM_ERR_LEVEL`   | Generic Go errors, panics, unknown issues | File I/O, JSON parse, panic recovery |

Define GBErrors using static mappings:

```go
KnownInternalErrors[CONFIG_FAIL_CODE] = &GBError{
  Code: CONFIG_FAIL_CODE,
  ErrLevel: INTERNAL_ERR_LEVEL,
  ErrMsg: "config checksum mismatch",
}
```

---

## How to Construct Errors

### Wrap a lower-level error with context

```go
return ChainGBErrorf(
  Errors.ConfigChecksumFailErr,
  err,
  "failed to verify config from peer %s", peerID,
)
```

### Add detail but no cause

```go
return ChainGBErrorf(
  Errors.DiscoveryReqErr,
  nil,
  "missing bootstrap peer info",
)
```

### Wrap a system-level error

```go
return WrapSystemError(err)
```

### Don't do this

```go
return fmt.Errorf("%w, %w", err1, err2) // Only one %w allowed
```

### Use fmt.Errorf for one-off, non-inspectable errors

```go
return fmt.Errorf("unexpected value in map: %v", val)
```

---

## Sending Errors

Use `.Net()` to prepare the error for network transmission:

```go
c.sendErr(reqID, respID, gbErr.Net())
// or
conn.Write(gbErr.ToBytes())
```

---

## Receiving Errors

### Parse from bytes

```go
err := BytesToError(msgBytes)
```

### Extract structured errors

```go
handled := HandleError(err, func(gbs []*GBError) error {
  for _, g := range gbs {
    log.Printf("received: %v", g)
  }
  return gbs[len(gbs)-1]
})

if errors.Is(handled, Errors.GossipDeferredErr) {
  // act accordingly
}
```

---

## Utility Summary

| Task                      | Use                                |
| ------------------------- | ---------------------------------- |
| Format and wrap error     | `ChainGBErrorf(...)`               |
| Wrap system error         | `WrapSystemError(...)`             |
| Convert to bytes          | `.Net()` / `.ToBytes()`            |
| Parse from wire           | `BytesToError(...)`                |
| Handle and select GBError | `HandleError(...)`                 |
| Check error level         | `ErrLevelCheck(...)`               |
| Get structured GBErrors   | `ExtractGBErrors + UnwrapGBErrors` |

---

## Decision Guide

| Scenario                             | Use                                   |
| ------------------------------------ | ------------------------------------- |
| Returning basic, unstructured error  | `fmt.Errorf(...)`                     |
| Wrapping a known protocol error      | `ChainGBErrorf(template, cause, msg)` |
| Wrapping a system-level failure      | `WrapSystemError(err)`                |
| Receiving structured error from wire | `BytesToError` + `HandleError`        |
| Sending structured error to remote   | `err.Net()`                           |

