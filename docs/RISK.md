# Risk and terms

The dio container image is **public**: `ghcr.io/malciller/dio-domains:latest`.
There is no approval step and no credentials are required to pull it.

By using dio you accept the following. They are the terms of use.

- dio places real orders and can lose money. There is no warranty.
- You take full responsibility and liability for your configuration, API keys,
  funds, and any legal, tax, or regulatory obligations that apply to you.
- You use dio entirely at your own risk. The author is not liable for any loss,
  damage, or cost arising from its use, including losses caused by software
  defects, outages, or incorrect behaviour.
- Do not redistribute the image with your own credentials or secrets baked in.

## Pull the image

Public images pull without a login:

```sh
docker pull ghcr.io/malciller/dio-domains:latest
```

You are then ready for [DEPLOYMENT.md](DEPLOYMENT.md).

## Support

Bugs and feature requests go through GitHub issues:
<https://github.com/malciller/dio-domains/issues>. There is no email or chat
support. Never paste API keys or other secrets into an issue.
