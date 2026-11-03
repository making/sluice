Difficulty: Low

# sluice-wicket-cf-api: rethink `cf app` showing `k/k` where the limit is above what is alive

Left as it is for now. The user asks only to look again for a better idea; nothing to change unless one is found.

`cf app` lists the instances alive, so below a limit of 4 with `warm` it shows `3/3`, and `0/0` with none, and the limit
shows nowhere in that line. The README's `cf app` bullet under Instances has why: the CLI counts the entries, so `3/4`
needs an entry for the rest, read as failed, and at none alive it keeps `cf push`'s wait until its timeout.

Weigh what the OSS CLI and the v3 API leave room for (the process's `instances` in `cf app`, `cf scale`'s output and
other commands, a stats entry's fields and states, `X-Cf-Warnings`). Report what was found: a change, or that this stays
and why.
