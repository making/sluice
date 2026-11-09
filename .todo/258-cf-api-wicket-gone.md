Difficulty: High

# sluice-wicket-cf-api: place elsewhere the instances of a wicket gone

After 257 (its moves). A wicket not answering (its host down, a crash loop) keeps its indices: the cf-api shows
them `STARTING` and puts the app when the wicket is back (`Controller::unreachable`), however long that takes.
Diego places a lost cell's instances elsewhere once its presence expires.

## To do

- A wicket unreachable past a bound (an option): its indices placed on the others with room, as an evacuation
  that removes nothing (the wicket answers nothing)
- Back, it serves its `routes.yaml` at once, through the server too, before the cf-api reaches it: an app moved
  away meanwhile, maybe of an older droplet or env, takes requests until the cf-api removes it. Weigh a wicket
  advertising an `api` app only once its controller listed or put it since its start, bounded so it still
  serves with the cf-api down (README Behavior: "The wicket serves on while the cf-api is down")
- Then the spread restored, as 257 does for a wicket back
- A wicket flapping moves no app back and forth; a partition (the cf-api cut off from a wicket the server
  still reaches) runs the moved instances twice until it heals: say so
- README Behavior

## Tests

The CLI test's two wickets: an app of `-i 1` whose wicket is killed (SIGKILL) runs on the other after the bound;
that wicket restarted, the app is removed from it, and a `-i 2` app's spread is restored.
