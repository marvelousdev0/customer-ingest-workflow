## Summary

<!-- What changed and why (1–3 bullets). -->

-

## Type of change

- [ ] Feature
- [ ] Bug fix
- [ ] Chore / docs / CI
- [ ] Hotfix / security

## Test plan

- [ ] `./gradlew test` (or relevant unit tests)
- [ ] Spotless / formatting clean
- [ ] Manual or integration verification (describe below)

## Risk & rollout

- **Blast radius:** <!-- packages / topics / stores touched -->
- **Feature flags / kill switch:** <!-- e.g. LaunchDarkly keys, defaults -->
- **Secrets / config:** <!-- Vault paths, env vars, k8s env -->
- **Rollback:** <!-- how to undo safely -->

## Checklist

- [ ] No real secrets committed (use placeholders / Vault)
- [ ] README / `.env.example` updated if config changed
- [ ] Tests cover new paths / flag off behavior

<!-- pr-intel:start -->
<!-- PR Intelligence fills complexity score + Mermaid impact flowchart here -->
<!-- pr-intel:end -->
