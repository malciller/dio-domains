# Security

## Reporting a vulnerability

dio handles exchange API keys and has access to live positions, so security
reports are taken seriously.

Please do **not** open a public issue for a security problem. Email
<security@diophantsolutions.com> instead, or use GitHub's private vulnerability
reporting on the repository (Security → Report a vulnerability) if available.

Include:

- the affected version (git tag or commit) and how to reproduce
- the impact, and whether keys, funds, or the runtime are exposed
- any proof-of-concept, sanitized of credentials

Reports are acknowledged within 72 hours and fixed in a private branch; the fix
ships in the next release and is attributed in the release notes unless you
prefer anonymity.

## What to keep out of this repository

- `.env` files (API keys, secrets). Only `.env.example` is tracked; CI fails
  builds that commit anything else.
- Private keys, mnemonic seeds, or wallet credentials in any file, issue, or
  comment.

## Disclosure policy

- Low/no-impact issues may be disclosed immediately once acknowledged.
- Issues with financial or operational impact: notify the maintainer first and
  allow a 30-day window (extendable by agreement) before public disclosure.

Verified reporters are added to the repository acknowledgments on request.