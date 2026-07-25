# Axum Result Response Design

## Goal

Provide a small, idiomatic Rust replacement for the upstream ASP.NET Core
`CatgaResult` HTTP-conversion extensions while preserving the existing stable
Catga error response contract.

## API

`catga-axum` will export an `IntoCatgaHttpResponse` extension trait for
`CatgaResult<T>` where `T: Serialize`. The trait will provide:

* `into_catga_response(status)`: serializes a successful value as an Axum JSON
  response with the supplied success status; a `204 No Content` success omits
  the body.
* `into_catga_created(location)`: returns a `201 Created` JSON response and
  supplies the caller-provided `Location` header.

An error always uses `CatgaHttpError`, so the status and compact
`{ "code", "message" }` payload remain identical to a direct error response.
The API consumes the result and location because an Axum response owns both.

## Deliberate Differences From C#

The upstream has overlapping `ToHttpResult`, `ToIResult`, and mutable
`ResultBuilder` APIs. Rust uses `Result::map`, `map_err`, and closures for
custom branching, so a builder would add mutable state, heap allocation, and
another error-mapping path without increasing expressiveness. The extension
trait therefore covers the framework boundary only.

Rust has no useful generic equivalent of C# success-with-null. Applications
that need an empty success response select `StatusCode::NO_CONTENT`; ordinary
`()` successes are still serialized consistently when another status is
selected.

## Error Handling And Performance

The implementation must not use `unwrap`, `expect`, or a fallback panic.
`HeaderValue::from_str` failures are impossible for a valid URI but remain
handled defensively as an internal Catga error response. Error results are
moved directly into `CatgaHttpError`; successful values are moved once into
`Json`, with no intermediate JSON buffer or cloned payload.

## Tests And Documentation

Integration tests must prove successful JSON responses, no-content behavior,
created responses with `Location`, and that error responses exactly reuse the
existing status/body mapping. Every public method receives Rustdoc with a
working example or enough contract detail to document ownership and status
semantics.
