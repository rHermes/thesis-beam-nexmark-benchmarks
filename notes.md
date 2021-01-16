# `--isRateLimited`

The `--isRateLimited` switch controls if the events generated should be heldback
to appear to be arriving in accordance with their event time or just fed whenever
the source is asked for a new event.

If turned on, events are put into a backlog if they are generated before their time.
There is also a backlog of events, meaning events which have been generated before this
moment in time, meaning events are backing up. This does **not** happen when not turning
on `--isRateLimited`.

When `--isRateLimted` is not turned on, the events generated still fall into the arch
set by the event parameters, but they are fed in at the rate the system can handle. This
becomes a push and not a poll variant then. The parameters which can still be tweaked is 
the number of generators through `--numEventGenerators` and the `--watermarkHoldbackSec`. 
Both of these might be interesting parametesr, since they might affect the runtime of the 
pipelines.

The watermarks are set to the eventtime of the latest event minus the
`--watermarkHoldbackSec` parameters value, which is 0 by default. The reason these
defaults work is because the `--outOfOrderGroupSize` parameter, which decides how many
elements are placed out of order, is set to 1 by default, which means there are no out
of order elements. If this is set to 1000, this implies that every 1000 event per
generator are emitted in pseudo-random order.

## What I have tried

For my tests I would like to use the `--isRateLimited` mode to simulate how real
events would interact with the system and to see when a backlog builds up. Initial
attempts to do this showed that there was a flaw in the system, turning on the
`--isRateLimited` option would cause most, 99% ish, of events to get dropped. For
100000 events generated 15 might get through on the passthrough query.

I spent quite some time figuring out how to turn on logging. It turns out that
the logger that beam uses by default is the jdk14 java logger, which doesn't support
the DEBUG or TRACE output levels. This was fixed by adding the simple logger
to the `build.gradle` file and use properties to set logging to what I wanted.

From this it became clear that the generator was being paused and resumed from
a checkpoint for almost every event. When this would happen, the delayed events
queue would not be restored, causing us to loose events. I wrote some code to
serialize the backlog into the checkpoints and with this I still lost about 1 in
every 10 events. This might have been proposional to the number of event generators,
but I don't know. *Maybe something to investigate next*. Worth noting that I tried
this with both the Direct and the Flink runnner and I also studied this in the
inteliji debugger.

I have a branch on `dragon` that is named `try-to-fix-ratelimiting` that contains
these fixes and attempts to fix the ratelimiting.

I gave up on trying to debug this further. I filed [a bug report](https://issues.apache.org/jira/browse/BEAM-11547)
with beam and wrote to the [mailing list](https://lists.apache.org/thread.html/r24e0541b33540c6c565f615989eeb64ef2c8dba4d1bd9b576b4a6128%40%3Cdev.beam.apache.org%3E).

# Debugging operators slowing things down

Another bump was the discovery that to count events Nexmark puts metric operators
before and after the operator pipeline, to count the events going in and out.
This is in effect an identity operator, which without my fix forces a deep copy
through serialization and deserialization. These two operators is playing to the
strengths of my fix and might bias results.

To try to combat this, I had an idea to disable these operators and instead use
flinks builtin metric system, to pull out the same kind of statistics that I would
with beam. This overhead would be the same with or without my fix and would maybe
give a better comparison between not enabling my fix and doing so.

I didn't manage to do this. Because of the way the source is coded, it seems to read
all events in the beginning and then the operator is a black box. I couldn't get the
records out or records in for any meaningful period of time.

I tried to code around this and so forth, but it didn't help. There might be something
more to this, but I couldn't get it to work.

It might be worth it to simply disable the operators all together and just look at the
runtime of the whole pipeline. I don't know if this will still cause the js files that
I rely on for data to be saved.

The option to toggle the debugging operators are `--debug`. I don't know how this works
in relation to the `--monitorJobs`, but I suspect that doesn't really work given that
flink jobs cannot be started and polled with beam, but once they are run, they will
block until completed.

The answer here is that I gave up again. 

# Kafka source

The kafka source seems promising in one way. If you use the combined mode, it will read
the events from the start, and they will arrive as fast as possible. If set in listen
only mode, then it will read from the latest point and continue until the number of events
has been reached.

I have not confirmed the as fast as possible point, but if I read it correctly that is the
way.

One way to hack this would be to create a middleware which reads in the events from a producer,
and on command just replays the pattern, but respects the events timings. I don't know how
this would be done with respecting the out of order requirements, but still. If we let
all of them be in order, then it could simply take the first one as the start point and
replay them relative to the first timestamp.

Not sure if I could write this middleware in go or it would have to be in java.

# Coder Strategy crashes when run from golang

Don't know why, going to check if running the flink cluster externally helps

This doesn't appear to have anything to do with golang, rather the number of events.

It crashes at 991684, events, no matter what. 
