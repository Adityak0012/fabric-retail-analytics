# Real-time anomaly alerts with Eventstream + Fabric Activator

Goal: get an email/Teams message **within a minute** when a store behaves abnormally — without
waiting for the daily batch.

## 1. Shape the stream (Eventstream editor)
Your Eventstream `es_pos_events` has the **Custom endpoint** source and a **Lakehouse** destination
(`bronze_pos_events`). Add a second branch for alerting:

1. **Group by** operator (after the source):
   * Aggregations: `SUM(line_amount)` as `revenue`, `COUNT(event_id)` as `lines`,
     `SUM(CASE event_type = 'return' → 1)` — if the UI can't express the conditional count, add a
     **Filter** operator `event_type == "return"` on a parallel branch and count there.
   * Group by: `store_id`
   * Window: **Tumbling, 5 minutes**
2. Destination: **Activator** → create new Activator item `act_retail_alerts`.

Tip: publish the Eventstream, run `python pos_stream_producer.py` for a few minutes, and confirm
windows appear in the Activator's data view before writing rules.

## 2. Rules in `act_retail_alerts`
Object = `store_id` (each store is monitored independently).

| Rule | Condition | Action |
|---|---|---|
| **Return spike** | `return_lines` in a 5-min window **> 15** | Email/Teams: "Return spike at {store_id}: {return_lines} returns in 5 min" |
| **Big-ticket basket** | `revenue` in a window **> 5,00,000** (local currency; adjust per store) | Email: possible fraud / pricing error |
| **Store gone silent** | no events from a store for **15 minutes** during trading hours (use Activator's *"no new events"* / absence condition) | Email: POS or network outage at {store_id} |

Keep thresholds in the rule description so reviewers see *why* each number was chosen.

## 3. Demo it
```bash
# normal traffic
python pos_stream_producer.py --rate 3
# in a second terminal: returns flood one store -> "Return spike" fires
python pos_stream_producer.py --anomaly return_spike --store S003 --rate 5 --minutes 6
# huge quantities -> "Big-ticket basket" fires
python pos_stream_producer.py --anomaly big_ticket --rate 3 --minutes 6
```
Screenshot the alert email + the Activator rule history for your README.

## 4. Cost control
Eventstream and Activator consume capacity units continuously while running. When you're not
demoing: stop the producer, **deactivate** the Eventstream source/destinations, and stop the rules.
