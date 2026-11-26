#!/usr/bin/env bash
# End-to-end check of the demo API against a running `docker compose up` stack.
# Usage: scripts/smoke.sh [base-url]   (needs curl and jq)
set -euo pipefail

BASE="${1:-http://localhost:8080}"
failures=0

step() { printf '\n== %s\n' "$*"; }
pass() { printf '   ok: %s\n' "$*"; }
fail() { printf '   FAIL: %s\n' "$*"; failures=$((failures + 1)); }

post() { curl -sS -X POST -H 'Content-Type: application/json' -d "$2" "$BASE$1"; }
state_of() { curl -sS "$BASE/jobs/$1" | jq -r '.state'; }

wait_for_state() { # id state timeout-seconds
	local id=$1 want=$2 deadline=$((SECONDS + $3)) state=""
	while ((SECONDS < deadline)); do
		state=$(state_of "$id")
		[[ "$state" == "$want" ]] && return 0
		sleep 0.5
	done
	printf '   job %s is "%s", wanted "%s"\n' "$id" "$state" "$want"
	return 1
}

step "API is up"
up=0
for _ in $(seq 1 90); do
	if curl -fsS "$BASE/" >/dev/null 2>&1; then up=1; break; fi
	sleep 1
done
((up == 1)) && pass "answered on $BASE" || { fail "no response from $BASE after 90 s"; exit 1; }

step "A job that fails twice is retried, then succeeds on attempt 3"
id=$(post /jobs '{"type":"email.send","queue":"emails","payload":{"to":"ana@example.com","subject":"retry","failFirst":2}}' | jq -r '.jobId')
if wait_for_state "$id" succeeded 120; then
	attempts=$(curl -sS "$BASE/jobs/$id" | jq -r '.attempts')
	[[ "$attempts" == "3" ]] && pass "succeeded after $attempts attempts" || fail "expected 3 attempts, saw $attempts"
else
	fail "job never succeeded"
fi

step "The same idempotency key returns the original job"
first=$(post /jobs '{"type":"report.generate","idempotencyKey":"smoke-report-1","payload":{}}')
second=$(post /jobs '{"type":"report.generate","idempotencyKey":"smoke-report-1","payload":{}}')
[[ "$(jq -r '.jobId' <<<"$first")" == "$(jq -r '.jobId' <<<"$second")" ]] && pass "same job id" || fail "different job ids"
[[ "$(jq -r '.created' <<<"$first")" == "true" && "$(jq -r '.created' <<<"$second")" == "false" ]] \
	&& pass "created true then false" || fail "created flags: $first / $second"

step "A permanent failure goes straight to the dead-letter state"
dead=$(post /jobs '{"type":"email.send","queue":"emails","payload":{"subject":"no recipient"}}' | jq -r '.jobId')
if wait_for_state "$dead" dead 60; then
	attempts=$(curl -sS "$BASE/jobs/$dead" | jq -r '.attempts')
	[[ "$attempts" == "1" ]] && pass "dead after a single attempt" || fail "expected 1 attempt, saw $attempts"
else
	fail "job never reached dead"
fi

step "Requeue works for a dead job and is refused for a finished one"
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE/jobs/$dead/requeue")
[[ "$code" == "202" ]] && pass "dead job requeued (202)" || fail "requeue of a dead job returned $code"
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE/jobs/$id/requeue")
[[ "$code" == "409" ]] && pass "succeeded job refused (409)" || fail "requeue of a succeeded job returned $code"

step "A workflow runs its steps in dependency order"
wf=$(post /workflows '{"idempotencyKey":"smoke-wf-1","steps":[
	{"key":"extract","type":"report.generate"},
	{"key":"transform","type":"report.generate","dependsOn":["extract"]},
	{"key":"validate","type":"report.generate","dependsOn":["extract"]},
	{"key":"load","type":"report.generate","dependsOn":["transform","validate"]}]}')
load=$(jq -r '.jobIds.load' <<<"$wf")
extract=$(jq -r '.jobIds.extract' <<<"$wf")
if wait_for_state "$load" succeeded 120; then
	finished_extract=$(curl -sS "$BASE/jobs/$extract" | jq -r '.completedAt')
	finished_load=$(curl -sS "$BASE/jobs/$load" | jq -r '.completedAt')
	[[ "$finished_extract" < "$finished_load" ]] && pass "extract finished before load" || fail "load finished before extract"
else
	fail "workflow never completed"
fi

step "A cycle is rejected"
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
	-d '{"steps":[{"key":"a","type":"x","dependsOn":["b"]},{"key":"b","type":"x","dependsOn":["a"]}]}' "$BASE/workflows")
[[ "$code" == "400" ]] && pass "400 for a dependency cycle" || fail "cycle returned $code"

step "Transactional outbox: the order and its e-mail job"
order=$(post /orders '{"customer":"ana","amount":1499.5}')
mail=$(jq -r '.confirmationJobId' <<<"$order")
if [[ -n "$mail" && "$mail" != "null" ]] && wait_for_state "$mail" succeeded 60; then
	pass "confirmation e-mail job succeeded"
else
	fail "confirmation job did not succeed: $order"
fi

step "Stats reflect what happened"
stats=$(curl -sS "$BASE/stats")
printf '   %s\n' "$stats"
[[ "$(jq -r '.succeeded' <<<"$stats")" -ge 6 ]] && pass "at least 6 succeeded" || fail "succeeded count too low"

echo
if ((failures > 0)); then
	echo "$failures check(s) failed"
	exit 1
fi
echo "all checks passed"
