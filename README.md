# watcher

Read-only, on-disk mirror of AWS SSM Parameter Store, kept fresh two ways at once:

- **Full sync** -- reconciles everything under a configured prefix on a fixed interval.
- **Event-driven sync** -- SSM emits an EventBridge event on every parameter change;
  `watcher` long-polls an SQS queue fed by that event and applies just the changed
  parameter(s) within seconds.

On any real change, from either path, it can run a configured command (e.g. to reload a
downstream app).

The provider layer is a small trait (`provider::Provider`) so other backends could be
added later; only Parameter Store is implemented today.

## Quick start

1. Write a config file (`config.toml`) -- see [`config.example.toml`](config.example.toml)
   for the full schema and defaults. At minimum you need a prefix and a destination root:

   ```toml
   [provider]
   type = "parameter_store"

   [provider.parameter_store]
   region = "us-east-1"
   prefix = "/myapp/prod"

   [destination]
   root = "/var/lib/watcher/params"

   [state]
   path = "/var/lib/watcher/state.json"

   [full_sync]
   interval = "5m"
   ```

2. If you want event-driven sync (recommended -- full sync alone means changes can take
   up to the full interval to show up), provision the SQS queue + EventBridge rule:

   ```sh
   watcher setup --config config.toml
   ```

   This prints a Queue URL. Paste it into `event_sync.queue_url` in your config, and set
   `event_sync.enabled = true`. `setup` is idempotent -- safe to re-run any time.

3. Run the daemon:

   ```sh
   watcher sync --config config.toml
   ```

   It runs one full sync immediately, then keeps both the full-sync ticker and the SQS
   event loop running until it receives SIGINT/SIGTERM, at which point it drains any
   in-flight `on_change` command before exiting.

## On-disk layout

One file per parameter, mirroring its SSM path under `destination.root`:

```
/myapp/prod/db/password  -->  <root>/myapp/prod/db/password
```

The file contains just the raw value. SecureString parameters are decrypted to plaintext
by default (`provider.parameter_store.decrypt = true`) -- see [`docs/iam.md`](docs/iam.md)
for the KMS permission that requires. Directories are created `0700` and files `0600`.

## The `on_change` command

Configured in `[on_change]`. Runs via `sh -c` whenever either sync path applies a real
change, with the changed parameter names available as environment variables:

- `WATCHER_CHANGED_PARAMS` -- comma-joined parameter names
- `WATCHER_CHANGED_COUNT` -- how many

A burst of rapid changes (e.g. several SQS messages arriving close together) is coalesced
into a single run via `on_change.debounce`.

## Known limitation: state file loss

`watcher` tracks a hash + version per parameter in `state.path` to tell a real change from
a no-op, and to know which parameter files it previously wrote (for pruning deletes). If
that file is lost while the on-disk parameter files remain, the next full sync treats
every parameter as new: it rewrites every file (same content, so harmless) and fires the
`on_change` command once. This is a known, low-risk edge case, not a bug to work around --
just be aware a lost state file causes one extra command run, not data loss.

## Development

```sh
cargo build
cargo clippy --all-targets -- -D warnings
cargo test
```

All unit tests run without AWS credentials (fake `Provider`/`Poller` implementations
stand in for SSM/SQS). `watcher setup` and the real `ParamStoreProvider`/`SqsPoller`
paths need a real AWS account to exercise end-to-end -- see the plan's verification
section for a manual checklist.
