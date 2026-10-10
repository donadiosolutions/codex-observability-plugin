# Langfuse Codex Plugin — Donadio fork

Codex plugin that sends OpenAI Codex session telemetry to Langfuse. It traces agent turns, model generations, reasoning summaries, system prompts, tool calls, images, subagent threads, skills, and token usage.

This fork uploads activity incrementally across all 12 supported Codex hooks.
The upstream project is [langfuse/codex-observability-plugin](https://github.com/langfuse/codex-observability-plugin).

Langfuse also documents the upstream integration on the [Codex integration page](https://langfuse.com/integrations/developer-tools/codex).

## Quick Start

Add the plugin marketplace:

```bash
codex plugin marketplace add donadiosolutions/codex-observability-plugin --ref <commit-sha>
codex plugin add tracing@donadio-codex-observability --json
```

Then enable hooks and the plugin in `~/.codex/config.toml`, or only for one project in `<project>/.codex/config.toml`:

```toml
[features]
hooks = true

[plugins."tracing@donadio-codex-observability"]
enabled = true
```

Restart Codex after changing the config. When **Hooks need review** appears, review the tracing hooks in `/hooks` and trust them. An installed and enabled plugin is not yet a trusted hook, and no traces are uploaded before you trust it. Codex records trust against the current hook hash, so a plugin update can require another review.

## Supported Versions

- A Codex build supporting all hooks below and `async: true` command hooks
  (the local `0.162.1` hook contract was inspected during validation)
- Node.js `22` and newer
- Linux with the supplied `flock` command for exclusive uploader ownership
- Git access to this fork; the marketplace uses the shipped standalone bundle
- Langfuse Cloud, or self-hosted Langfuse `3.95.0` and newer

## Delivery

Every invocation records a hook event and uploads changed observations. Active
turns keep stable observation IDs as their input, output, usage, and completion
state change. Hook payloads cover transcript-flush races; child uploads resolve
their parent trace so a child can finish after its parent turn.

All hooks run asynchronously except `SessionEnd`, which attempts a final
drain before Codex cancels outstanding async work. Codex limits `SessionEnd`
and `Interrupt` to three seconds; pending payloads are saved before transcript
discovery and remain retryable if that deadline interrupts delivery. The supported events
are `PreToolUse`, `PermissionRequest`, `PostToolUse`, `PreCompact`, `PostCompact`,
`SessionStart`, `SessionEnd`, `UserPromptSubmit`, `SubagentStart`, `SubagentStop`,
`Stop`, and `Interrupt`.

Exports flush at 16 observations, an 8 MiB encoded request budget, or one second,
whichever comes first. A larger individual observation uses a single request
up to 256 MiB without truncation. Requests run sequentially. Failed or partially
acknowledged uploads remain retryable. Pending hook payloads and acknowledged
observation fingerprints are stored privately beside the root transcript;
concurrent invocations serialize delivery with a bounded lock.

Delivery follows hook events; Codex currently has no hook for each streamed
model token. Langfuse visibility also includes its ingestion processing time.

## Langfuse Credentials

Create `~/.codex/langfuse.json` (global) or `<project>/.codex/langfuse.json` (per-project) with your Langfuse credentials.

```json
{
  "enabled": true,
  "public_key": "pk-lf-...",
  "secret_key": "sk-lf-...",
  "base_url": "https://cloud.langfuse.com"
}
```

Only `enabled`, `public_key` and `secret_key` are required. If `base_url` is not set, the plugin uses `https://cloud.langfuse.com` for the 🇪🇺 EU region. The other regions are `https://us.cloud.langfuse.com` (🇺🇸 US), `https://jp.cloud.langfuse.com` (🇯🇵 Japan) and `https://hipaa.cloud.langfuse.com` (⚕️ HIPAA).

You can also set credentials with environment variables:

```bash
export TRACE_TO_LANGFUSE="true"
export LANGFUSE_PUBLIC_KEY="pk-lf-..."
export LANGFUSE_SECRET_KEY="sk-lf-..."
export LANGFUSE_BASE_URL="https://cloud.langfuse.com"
```

Tracing stays off until `enabled` or `TRACE_TO_LANGFUSE` is true, so you opt in explicitly. Config is resolved as defaults, then `~/.codex/langfuse.json`, then `<project>/.codex/langfuse.json`, then environment variables, and the environment wins. `LANGFUSE_CODEX_*` variables take precedence over the matching `LANGFUSE_*` ones, so you can scope credentials to Codex without disturbing other Langfuse tooling on the same machine.

The remaining options are settable both ways, as a `langfuse.json` key or as the matching `LANGFUSE_CODEX_*` variable:

- `environment` labels the traces with an environment, for example `production`.
- `user_id` attaches a user to every trace. It defaults to the Codex auth email, if one is found.
- `tags` adds your own tags to every trace, either as a JSON array or as a comma-separated list.
- `metadata` attaches a JSON object to every trace.
- `skill_tags` tags traces with `skill:<name>` for every skill invoked in the turn, and defaults to `true`.
- `trace_seed` derives deterministic trace ids, so a headless caller knows a run's trace id up front. Use a unique seed per session.
- `debug` logs verbosely to stderr, and defaults to `false`.
- `fail_on_error` fails the hook on upload errors instead of failing open, and defaults to `false`.

Everything the plugin traces is uploaded to Langfuse, including prompts and tool inputs and outputs, so do not enable it for sessions containing data you do not want stored there.

## Build and review

The Git marketplace ships `dist/index.mjs`. CI rebuilds it and requires byte
identity with the committed artifact. CodeQL analyzes the TypeScript source
and tests; the generated dependency bundle is excluded from duplicate source
analysis. Dependency integrity remains pinned in `pnpm-lock.yaml` and checked
by Socket.

## Contributing

See the [contributing guide](./CONTRIBUTING.md).

## License

[MIT](./LICENSE)
