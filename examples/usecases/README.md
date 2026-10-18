# Use cases

Four worked examples, each a realistic stream of events delivered in more
than one arrival order. Each one also runs the kind of consumer people write
by hand on the same arrivals, so you can see what goes wrong without
Inorderly. The posts on [inorderly.com](https://inorderly.com/tags/use-cases/)
walk through them, and every number in those posts comes from
[test/results/usecases.txt](../../test/results/usecases.txt).

Check all four and print the report (about 20 seconds):

```sh
go run ./examples/usecases
```

Write the input files too, then order them with the command:

```sh
go run ./examples/usecases -dir uc
cd uc/webhooks
inorderly -window 60000 -in deliveries-1.ndjson -out deliveries-1.out.ndjson -rejects deliveries-1.rejects.ndjson
```

Every command for one example prints the same replay hash. Name examples to
check only those: `go run ./examples/usecases -dir uc feeds audit`. The files
take about 180 MB in all. Everything comes from fixed seeds, so you get the
same files and the same numbers every time.

## Webhooks

20,000 shop orders. Each has an order event from checkout, a payment that
names the order as its parent, and then a shipment or, for about one order
in twelve, a refund. The payments service's clock runs up to 1.5 seconds
behind, so 3,487 payments are stamped before their order. The webhook
provider retries about 15% of deliveries, some twice. The same events are
delivered three times with different delays and retries.

```sh
inorderly -window 60000 -in deliveries-1.ndjson -out deliveries-1.out.ndjson -rejects deliveries-1.rejects.ndjson
inorderly -window 60000 -in deliveries-2.ndjson -out deliveries-2.out.ndjson -rejects deliveries-2.rejects.ndjson
inorderly -window 60000 -in deliveries-3.ndjson -out deliveries-3.out.ndjson -rejects deliveries-3.rejects.ndjson
```

Compared with: summing payments and refunds as they arrive, and sorting by
timestamp after dropping repeats.

## Feeds

150,000 ticks for three symbols, sent over two lines that each lose about
2% of their ticks, plus a 4-second outage on line A and a 3-second one on
line B. Merged live twice with different line delays, and once from the two
capture files read side by side.

```sh
inorderly -window 10000 -in live-1.ndjson -out live-1.out.ndjson -rejects live-1.rejects.ndjson
inorderly -window 10000 -in live-2.ndjson -out live-2.out.ndjson -rejects live-2.rejects.ndjson
inorderly -window 10000 -in line-a.ndjson -in line-b.ndjson -out capture.out.ndjson -rejects capture.rejects.ndjson
```

Compared with: passing on each tick the first time its number is seen.

## Telemetry

Three hours of readings, one every 10 seconds, from 200 devices with clocks
up to 3 seconds out. Fifty devices sit behind gateways that forward in
batches every 45 to 90 seconds, twenty go offline for 2 to 12 minutes, ten
for 25 to 40 minutes, and about 3 readings in 1,000 are lost. Uploaded three
times with different delays and gateway timing.

```sh
inorderly -window 900000 -in uploads-1.ndjson -out uploads-1.out.ndjson -rejects uploads-1.rejects.ndjson
inorderly -window 900000 -in uploads-2.ndjson -out uploads-2.out.ndjson -rejects uploads-2.rejects.ndjson
inorderly -window 900000 -in uploads-3.ndjson -out uploads-3.out.ndjson -rejects uploads-3.rejects.ndjson
```

Compared with: averaging each minute and closing it once readings a minute
past its end have arrived.

## Audit

About 100,000 transactions on 300 accounts at four branches with clocks up
to 0.8 seconds apart: deposits, withdrawals that are declined when the
balance is short, and reversals that name the transaction they undo. Two
systems, the ledger and billing, get them through queues with different
delays. An auditor replays the four branch logs, exported at least once, so
some lines are there twice.

```sh
inorderly -window 600000 -in ledger.ndjson -out ledger.out.ndjson -rejects ledger.rejects.ndjson
inorderly -window 600000 -in billing.ndjson -out billing.out.ndjson -rejects billing.rejects.ndjson
inorderly -window 600000 -in branch-1.ndjson -in branch-2.ndjson -in branch-3.ndjson -in branch-4.ndjson -out audit.out.ndjson -rejects audit.rejects.ndjson
```

Compared with: each system applying transactions in the order it gets them.

The code is in [internal/usecase](../../internal/usecase).
