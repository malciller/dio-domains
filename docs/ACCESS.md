# Access to the prebuilt image

The dio container image is private. Requests are approved at the maintainer's
discretion.

## Request access

Open a request:

**<https://github.com/malciller/dio-domains/issues/new?template=image-access-request.yml>**

Give your GitHub username and what you intend to run. Do not paste API keys or
other secrets into the issue.

## What happens next

1. A maintainer reviews the request and applies the `access-approved` label.
   That pings the maintainer with the package settings link; nothing is sent to
   you yet.
2. The maintainer grants your account the **Read** role and applies the
   `access-granted` label.
3. That posts the pull instructions on the issue and closes it.

The invite is manual because GitHub's API only grants package access for
organization-scoped packages, not personal-account ones.

If the request is declined, the issue is closed with a short reply.

## Pull the image

Create a **personal access token (classic)** with the `read:packages` scope,
then:

```sh
echo "$GHCR_PAT" | docker login ghcr.io -u YOUR_GITHUB_USERNAME --password-stdin
docker pull ghcr.io/malciller/dio-domains:latest
```

You are then ready for [DEPLOYMENT.md](DEPLOYMENT.md).

## Terms

Access is conditional on accepting these, recorded as checkbox acknowledgements
on your request issue:

- dio places real orders and can lose money. There is no warranty.
- You take full responsibility and liability for your configuration, API keys,
  funds, and any legal, tax, or regulatory obligations that apply to you.
- You use dio entirely at your own risk. The author is not liable for any loss,
  damage, or cost arising from its use, including losses caused by software
  defects, outages, or incorrect behaviour.
- Do not redistribute the image or your pull credentials.

To have access revoked, say so on your request issue.
