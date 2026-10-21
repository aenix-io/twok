# HTTP arbiter contract

A service that implements this contract can be a twok arbiter with `--arbiter https://host/path`.
`twok arbiter serve` is a reference implementation that keeps its records in a directory.

## Resources

Every path is one record.
twok uses the path in its URL for the record and the same path with the suffix `.probe` for the startup check, so the service must accept both.
A record body is a JSON object, at most 64 KiB, in practice a few hundred bytes.
The service stores it as it is and returns it unchanged; it does not need to understand it.

## Requests

### Read

```http
GET /clusters/prod HTTP/1.1
Accept: application/json
```

When the record exists, the answer is `200` with the body and an `ETag`:

```http
HTTP/1.1 200 OK
Content-Type: application/json
ETag: "5f1d0c3a..."

{"schemaVersion":1,"clusterID":"cdf818194e3a8c32","generation":3,"survivor":"a","writeID":"9b2e..."}
```

When it does not, the answer is `404`.

### Create

```http
PUT /clusters/prod HTTP/1.1
If-None-Match: *
Content-Type: application/json

{"schemaVersion":1,"generation":1,"survivor":"a","writeID":"0c4f..."}
```

The answer is `201` (or `200`) with the new `ETag` when the record did not exist, and `412` when it did.

### Swap

```http
PUT /clusters/prod HTTP/1.1
If-Match: "5f1d0c3a..."
Content-Type: application/json

{"schemaVersion":1,"generation":4,"survivor":"b","writeID":"77aa..."}
```

The answer is `200` with the new `ETag` when the current `ETag` equals the one in `If-Match`, and `412` when it does not.
When the record does not exist, the answer should be `412`; twok also accepts `404` and treats it as `412`.

## Requirements

- The comparison and the write must be one atomic step: of two concurrent requests with the same condition, at most one may succeed.
- The `ETag` must change with every successful write. A content hash is enough, because every body twok writes carries a fresh `writeID`.
- A `PUT` without `If-None-Match` or `If-Match` should be refused, with `428`. twok never sends one.
- `GET` must see every write that was answered with a `2xx`.
- Any other answer, such as `5xx`, `401` or a timeout, tells twok nothing: it reads the record again before it decides.

A service that answers `200` to a conditional `PUT` it did not evaluate is not an arbiter.
twok checks this at startup on the probe path, and refuses to run against such a service.

## Authentication

When `TWOK_HTTP_ARBITER_TOKEN_FILE` is set, every request carries `Authorization: Bearer <token>` with the file's content.
The file is read on every request, so it can be rotated in place.
A token is sent only over `https`.
Credentials in the URL are refused.

## Timeouts and retries

Every request has a timeout of 10 seconds.
twok does not retry a `PUT` itself.
When a `PUT` gets no answer, or an answer other than `2xx`, twok reads the record back: if the stored body carries the `writeID` of that `PUT`, the write happened.
So a proxy that retries a `PUT` and answers `412` for the retry of a write that was applied does no harm.
