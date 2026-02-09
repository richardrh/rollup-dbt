# Install on your system

It is recommended on Hiscox system to use scoop to install your require
packages as below. Although users might be comfortable with their
own system, the only requirement is:

1. git
2. uv
3. duckdb

# Quick Install

### Via Scoop

```bash
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
Invoke-RestMethod -Uri https://get.scoop.sh | Invoke-Expression

scoop install git
scoop install uv
scoop install duckdb
```
Once you have git and uv installed you will pull the repo
from Hiscox Bitbucket.

```bash

git clone [insert]
uv sync

```

If you have issues with TLS certs then run this instead.
```bash
uv sync [tls]
```

