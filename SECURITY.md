# Security

## Reporting a vulnerability

dio holds exchange API keys and live positions. Take security seriously.

Do not open a public issue for a security problem. Email
<security@diophantsolutions.com>, or use GitHub's private vulnerability
reporting (Security tab, Report a vulnerability).

Include:

- the version or commit, and how to reproduce
- what is exposed, keys, funds, runtime
- a proof of concept, with credentials removed

Reports get a reply within 72 hours. The fix ships in the next release and gets
credited in the release notes, unless you want to stay anonymous.

## What to keep out of this repo

- `.env` files. Only `.env.example` is tracked. CI fails builds that commit the
  rest.
- Private keys, seeds, or wallet credentials in any file, issue, or comment.

## Disclosure policy

- Low impact stuff can go public right after a reply.
- If it touches money or operations, the maintainer gets 30 days before public
  disclosure. You can extend that if you both agree.

Verified reporters can get a mention in the repo's acknowledgments on request.