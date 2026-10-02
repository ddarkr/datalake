# Security policy

## Reporting a vulnerability

Report security issues privately through GitHub:

https://github.com/ddarkr/datalake/security/advisories/new

Do not open a public issue or PR comment for a suspected vulnerability.

## What to include (and what not to)

- Include: affected file or service, steps to reproduce, and impact.
- Do NOT include: tokens, passwords, raw CAN captures, VINs, vehicle
  identifiers, or private server addresses. Redact them before sending; the
  maintainer cannot un-publish what a report already exposed.

## Scope notes

- This project handles vehicle telemetry and credentials-adjacent config; treat
  any `.env` content and vehicle data as sensitive by default.
- No supported-version guarantee or response-time SLA is stated here. Reports
  are reviewed on a best-effort basis by the maintainer.
