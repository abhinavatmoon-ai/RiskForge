# RiskForge API Gateway

The API Gateway is the only public HTTP entry point for RiskForge. It receives client transaction requests, verifies the caller's JWT, applies a per-client rate limit, and forwards valid requests to the ingestion service.

It is intentionally a **thin gateway**. It does not validate transaction business rules, persist transactions, publish Kafka messages, or calculate fraud scores. Those responsibilities belong to the downstream services.

## What this service does

| Capability | Behaviour |
| --- | --- |
| Routing | Forwards `/api/v1/transactions/**` to `ingestion-service` at `http://localhost:8081`. |
| Authentication | Requires an HS256 JWT in the `Authorization: Bearer <token>` header. |
| Trusted identity forwarding | Copies the validated JWT subject into `X-Auth-User-Id` for the downstream service. Any client-supplied version of this header is removed first. |
| Rate limiting | Uses Redis to allow 10 requests/second per source IP, with a burst capacity of 20. |
| Resilience | Uses a circuit breaker. If ingestion is unreachable, responds with `503 Service Unavailable`. |
| Observability | Exposes `/actuator/health` and `/actuator/info`. |

## Request flow

```text
Client
  |
  | POST /api/v1/transactions
  | Authorization: Bearer <JWT>
  v
API Gateway :8080
  |-- AuthenticationFilter: validates JWT and adds X-Auth-User-Id
  |-- RequestRateLimiter: stores counters in Redis
  |-- CircuitBreaker: handles an unavailable ingestion service
  v
Ingestion Service :8081
```

## Low-level design (LLD)

```text
com.riskforge.api_gateway
|
|-- ApiGatewayApplication
|     Starts the reactive Spring Boot application.
|
|-- filter/AuthenticationFilter
|     A Spring Cloud Gateway filter factory.
|     1. Reads Authorization header.
|     2. Requires the Bearer scheme.
|     3. Verifies the HS256 JWT signature and expiry with JJWT.
|     4. Removes untrusted X-Auth-User-Id.
|     5. Adds the JWT subject as X-Auth-User-Id and continues the filter chain.
|
|-- config/GatewayConfiguration
|     Supplies ipKeyResolver, which returns the caller IP for Redis rate-limit keys.
|
|-- controller/GatewayFallbackController
|     Serves the internal /fallback/ingestion route with a JSON 503 response.
|
`-- application.yml
      Declares the HTTP route and its filters, Redis location, JWT secret, and Actuator exposure.
```

### Filter order

For a transaction request, the configured filter chain is:

1. `AuthenticationFilter` rejects missing, malformed, expired, or incorrectly signed JWTs with `401 Unauthorized`.
2. `RequestRateLimiter` uses the source IP as its Redis key and rejects excess traffic with `429 Too Many Requests`.
3. `CircuitBreaker` forwards traffic to ingestion. If it cannot reach the service, it forwards internally to `/fallback/ingestion`, which returns `503`.

## Technology

- Java 21
- Spring Boot 3.3.4
- Spring Cloud Gateway (reactive/WebFlux)
- Resilience4j circuit breaker
- Redis reactive client for rate limiting
- JJWT 0.12.5 for HS256 JWT validation
- Spring Boot Actuator

## Configuration

The default development configuration is in `src/main/resources/application.yml`.

| Setting | Environment variable | Default | Description |
| --- | --- | --- | --- |
| Gateway port | — | `8080` | Gateway HTTP port. |
| Redis host | `REDIS_HOST` | `localhost` | Redis host used by the rate limiter. |
| Redis port | `REDIS_PORT` | `6379` | Redis port used by the rate limiter. |
| JWT secret | `JWT_SECRET` | Development-only value | Secret used to verify HS256 JWTs. |
| Ingestion URI | — | `http://localhost:8081` | Destination for transaction requests. |

Never use the fallback JWT secret outside local development. Set a long random `JWT_SECRET` through your shell, Docker/Kubernetes secret, or CI/CD environment.

## Prerequisites

1. JDK 21.
2. Redis 7+ running on `localhost:6379` (or set `REDIS_HOST` and `REDIS_PORT`).
3. The ingestion service running on `localhost:8081` for successful end-to-end forwarding.

Start Redis locally with Docker:

```powershell
docker run --rm --name riskforge-redis -p 6379:6379 redis:7-alpine
```

Set a local JWT secret before starting the gateway:

```powershell
$env:JWT_SECRET = "replace-with-a-local-secret-at-least-32-bytes-long"
```

## Run the service

```powershell
./mvnw.cmd spring-boot:run
```

If Maven Wrapper cannot locate its local repository on Windows, set it explicitly and rerun:

```powershell
$env:MAVEN_USER_HOME = "$env:USERPROFILE\.m2"
./mvnw.cmd spring-boot:run
```

The gateway starts at `http://localhost:8080`.

## Test the service

### 1. Build and run unit/context tests

```powershell
./mvnw.cmd test
```

### 2. Check health

```powershell
Invoke-RestMethod http://localhost:8080/actuator/health
```

Expected result: HTTP `200` and a JSON health response. Redis must be available for the Redis health contributor to report `UP`.

### 3. Verify authentication rejection

This request has no JWT and should return `401 Unauthorized` before it reaches the ingestion service.

```powershell
Invoke-WebRequest -Method Post `
  -Uri http://localhost:8080/api/v1/transactions `
  -ContentType 'application/json' `
  -Body '{"accountId":"ACC-10001","amount":12500.00,"currency":"INR"}'
```

### 4. Generate a local development token

Use this only with a local development secret. It creates an HS256 JWT with the subject `user-1001` and a one-hour expiry.

```powershell
function ConvertTo-Base64Url([byte[]] $bytes) {
  [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

$secret = $env:JWT_SECRET
$header = ConvertTo-Base64Url([Text.Encoding]::UTF8.GetBytes('{"alg":"HS256","typ":"JWT"}'))
$expiresAt = [DateTimeOffset]::UtcNow.AddHours(1).ToUnixTimeSeconds()
$payload = ConvertTo-Base64Url([Text.Encoding]::UTF8.GetBytes("{`"sub`":`"user-1001`",`"exp`":$expiresAt}"))
$unsignedToken = "$header.$payload"
$hmac = [Security.Cryptography.HMACSHA256]::new([Text.Encoding]::UTF8.GetBytes($secret))
$token = "$unsignedToken.$(ConvertTo-Base64Url($hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($unsignedToken))))"
```

### 5. Send an authenticated transaction

With Redis and ingestion running, this request should be forwarded to `localhost:8081`. The exact success response is owned by the ingestion service.

```powershell
$headers = @{ Authorization = "Bearer $token" }
Invoke-RestMethod -Method Post `
  -Uri http://localhost:8080/api/v1/transactions `
  -Headers $headers `
  -ContentType 'application/json' `
  -Body '{"accountId":"ACC-10001","cardId":"CARD-90001","amount":12500.00,"currency":"INR","merchantId":"MERCHANT-100","latitude":19.0760,"longitude":72.8777,"timestamp":"2026-08-26T10:30:00Z"}'
```

### 6. Verify the fallback response

Stop the ingestion service while Redis remains available, then repeat the authenticated request. The gateway should return:

```json
{
  "code": "INGESTION_UNAVAILABLE",
  "message": "Transaction intake is temporarily unavailable. Please retry later."
}
```

with HTTP status `503`.

### 7. Verify rate limiting

Send more than 20 requests quickly from the same machine. Once the token bucket is exhausted, the gateway should return HTTP `429 Too Many Requests`. Wait briefly for the 10-per-second refill before trying again.

## API surface

| Method | Path | Authentication | Purpose |
| --- | --- | --- | --- |
| `POST` | `/api/v1/transactions/**` | Required | Routes transaction requests to ingestion. |
| `GET` | `/actuator/health` | Not configured at gateway level | Health check. |
| `GET` | `/actuator/info` | Not configured at gateway level | Basic service information. |

## Current boundaries and next improvements

- The gateway accepts HS256 shared-secret JWTs for simplicity. A production system should use an identity provider, public-key verification (JWKS), issuer/audience checks, and key rotation.
- IP-based rate limiting is appropriate for a starter project. After authentication, consider rate-limiting by customer or API key, taking trusted reverse proxies into account.
- The transaction route currently matches every HTTP method. Add a `Method=POST` predicate when the ingestion API contract is finalized.
- The gateway does not retry transaction submissions. This is deliberate: blindly retrying financial writes can create duplicates. Add idempotency keys before adding retry behaviour.
- Keep internal services off the public network; clients should enter RiskForge only through this gateway.
