# The ordering contract

This is what Inorderly promises, exactly. Read it before changing how the
Orderer behaves: every rule here is something a user can rely on, and the
tests check them.

## The promise

Give an Orderer a set of events. If no event arrives more than `Window` behind
the newest event time seen so far, the output is the same sequence, byte for
byte, whatever order the events arrived in, however many times each one
arrived, and however the stream was cut up by snapshots and restores. The
replay hash, the SHA-256 of the output, is the same too.

Events that break that bound are refused as late. They never change the
output of the events that were on time.

## Events

| Field | Type | Meaning |
|---|---|---|
| `producer` | string, required | Who made the event. |
| `time` | integer, required | Event time, in units you choose. Milliseconds since 1970 are common. |
| `id` | string | The event's identity. |
| `seq` | integer above 0 | The event's number within its producer. It is the identity when `id` is empty. |
| `logical` | integer, 0 to 4294967295 | The logical counter of a hybrid logical clock. It orders events with the same `time`. |
| `parents` | list of identities | Events that must come before this one. |
| `data` | any JSON value | The payload. |

An event needs `id` or `seq`. Its **identity** is `id`, or else
`producer#seq`. Parents name identities. Identities, producers and parents
must be valid UTF-8. An input line's keys must be spelled exactly as above,
each at most once.

Every event is put in a **canonical form**: fields in the order above, empty
ones left out, `data` with no whitespace, object keys sorted by their bytes,
strings escaped one way (quote, backslash and control characters escaped,
everything else as UTF-8), and numbers exactly as written. `"data":null` is
the same as no data. Two arrivals with the same identity and the same
canonical form are the same event.

## The watermark

The **watermark** is the newest `time` seen, minus `Window`. It only moves
forward.

- An event whose `time` is below the watermark when it arrives is **late**. It
  is refused and changes nothing.
- An event at or above the watermark is **on time**.

`Window` 0 still allows events with the same time as the newest one.

## The order

Each event has a **key**: a list of points, compared one point at a time, with
a shorter key that is a prefix of a longer one coming first.

An event's **own point** is compared by `time`, then `logical`, then
`producer` (by bytes), then `seq`, then `id` (by bytes). Different identities
never have the same own point.

A **deadline point** for time *t* comes after every own point with time *t*
and before every own point with a later time.

Each event has a **deadline**: its `time` plus `Hold`.

An event's key is the largest of:

- its own point, alone;
- for each parent that is in the set, whose key comes before the event's
  deadline point and holds fewer than 64 points: the parent's key with the
  event's own point added. That puts the event straight after its parent;
- for each parent that never comes, comes at or after the event's deadline,
  is more than `Remember` older than the event (when `Remember` is above 0),
  or whose key already holds 64 points: the event's deadline point followed
  by its own point. That puts the event at its deadline.

The 64-point limit (`MaxChain`) only matters for a run of more than 63
events in a row, each with a clock behind its parent's. It keeps keys short
whatever a producer sends.

Events come out in key order. When events wait on each other in a cycle,
the one with the earliest deadline (then the lowest own point) is settled
first, at its deadline, as if the parents it is still waiting for were
missing. Once it has a key, the others follow from it.

An event is released as soon as its key's first point is below the
watermark. `Flush` releases everything still held, as if the watermark had
gone past every time.

Arithmetic on times saturates at the ends of the 64-bit range instead of
wrapping.

## What each output line says

Each output line is the event's canonical form with two more fields:

- `pos`: its position in the output, from 1;
- `missing`: the parents that weren't released before it, in the order the
  event names them. A parent more than `Remember` older than the event counts
  as missing.

Gaps in each producer's `seq` numbers, as they come out, are reported in the
summary: from the first `seq` seen, each jump past the next number is a gap,
and a `seq` that goes backwards is counted separately.

## Duplicates and conflicts

- The same event again, while it is held or remembered, is a **duplicate**: it
  is dropped and counted.
- An event with the same identity but a different canonical form is a
  **conflict**: it is refused and counted, and the first one to arrive is
  kept. The output then depends on which arrived first, so conflicts are
  outside the promise. The rejects file and the summary show them.

## Memory

`Remember` is how far behind the watermark released events are remembered,
for spotting duplicates and finding parents. 0 remembers them all. With
`Remember` above 0, an event is forgotten only when no held event could still
name it as a parent less than `Remember` older than itself. A copy of a
forgotten event that arrives later is late anyway, so it is refused as late
rather than counted as a duplicate.

## Snapshots

`Snapshot` takes the whole state between two pushes. `SnapshotHeld` leaves out
the released events it remembers; keep the `Record` of every released event
instead and hand them to `Restore`. A restored Orderer releases exactly what
the original would have released next.

## What is outside the promise

- Late events, and conflicts: both are reported, never hidden.
- Changing `Window`, `Hold` or `Remember` part way through a stream.
- Two Orderers fed different sets of events.
